//
//  Synchronizer.swift
//  FindMySync
//
//  Created by ZZZ on 17/01/23.
//

import AXSwift
import CryptoKit
import Foundation
import UniformTypeIdentifiers

class Synchronizer {
    var log: (_ message: String) -> Void
    var logConfig: (_ config: String) -> Void
    var clearLog: () -> Void
    var onAccessDenied: () -> Void
    var timer: Timer?
    var retried: Int = 0

    static let shared = Synchronizer(
        log: { message in
            debugPrint(message)
        },
        clearLog: {
            debugPrint("========")
        },
        logConfig: { message in
            debugPrint(message)
        },
        onAccessDenied: {
            debugPrint("ACCESS DENIED")
        }
    )

    public init(
        log: @escaping (_ message: String) -> Void, clearLog: @escaping () -> Void,
        logConfig: @escaping (_ message: String) -> Void,
        onAccessDenied: @escaping () -> Void
    ) {
        self.log = log
        self.logConfig = logConfig
        self.clearLog = clearLog
        self.onAccessDenied = onAccessDenied
    }

    func fetchData() {
        clearLog()

        let date = Date()
        let dateFormatter = DateFormatter()
        dateFormatter.dateFormat = "yyyy-MM-dd HH:mm:ss"

        log(dateFormatter.string(from: date) + "\n")

        MQTTPublisher.shared.log = { message in
            DispatchQueue.main.async { self.log(message) }
        }
        MQTTPublisher.shared.begin()

        let hide_findmy_app: Bool = UserDefaults.standard.bool(
            forKey: "extra_hide_findmy_app")
        let generate_config: Bool = UserDefaults.standard.bool(
            forKey: "extra_generate_config")

        if hide_findmy_app {
            if UIElement.isProcessTrusted() {
                if let app = Application.allForBundleID("com.apple.findmy").first {
                    log("Found FindMy app, gonna hide it")
                    do {
                        //                        try app.setAttribute(.hidden, value: true)

                        try app.windows()?.first?.setAttribute(
                            .size, value: CGSize(width: 0, height: 0))

                        if let mainScreen = NSScreen.main {
                            try app.windows()?.first?.setAttribute(
                                .position,
                                value: CGPoint(
                                    x: mainScreen.visibleFrame
                                        .width,
                                    y: mainScreen.visibleFrame
                                        .height))
                        }

                    } catch {
                        log("Cannot hide FindMy app: \(error)")
                    }
                }
            } else {
                log(
                    "Cannot hide FindMy app because Accessibility API permission was not granted"
                )
            }
        }

        let homeDirectory = URL(fileURLWithPath: NSHomeDirectory())
        log("Home directory: " + homeDirectory.absoluteString)

        var hasAccess = true
        var haConfig = ""
        var publishedCount = 0
        // Set only by a path that actually opened the Find My cache. The
        // heartbeat is withheld otherwise, so a missing beacon key or both
        // sources switched off reads as stale in Home Assistant, as it should.
        var readFindMy = false

        if #available(macOS 14.4, *) {
            func getKeyData(from hexString: String) -> Data {
                let genaHexString = hexString.trimmingCharacters(in: .whitespacesAndNewlines)
                    .replacingOccurrences(of: "0x", with: "")

                var data = Data(capacity: genaHexString.count / 2)
                var index = genaHexString.startIndex

                while index < genaHexString.endIndex {
                    guard genaHexString.distance(from: index, to: genaHexString.endIndex) >= 2 else {
                        break
                    }
                    let byteString = genaHexString[index..<genaHexString.index(index, offsetBy: 2)]
                    let byte = UInt8(byteString, radix: 16)!
                    data.append(byte)
                    index = genaHexString.index(index, offsetBy: 2)
                }

                return data
            }

            var keyData: Data = Data()
            if let beacon_key: String = UserDefaults.standard.string(forKey: "extra_beacon_key") {
                if !beacon_key.isEmpty {
                    log("Getting Beacon key from preferences...")

                    let data = getKeyData(from: beacon_key)

                    keyData = Data(data)
                }
            }

            if keyData.count == 0 {
                log("Getting Beacon key from keychain...")
                let query: [String: Any] = [
                    kSecClass as String: kSecClassGenericPassword,
                    kSecAttrLabel as String: "BeaconStore",
                    kSecMatchLimit as String: kSecMatchLimitOne,
                    kSecReturnAttributes as String: true,
                    kSecReturnData as String: true,
                ]

                var item: CFTypeRef?
                let status = SecItemCopyMatching(query as CFDictionary, &item)
                if status == errSecSuccess, let existingItem = item {
                    if let data = existingItem[kSecValueData as String] as? Data {
                        keyData = Data(data)
                    }
                }
            }

            if keyData.count == 0 {
                log("Getting Beacon key from command...")
                let task = Process()
                task.launchPath = "/usr/bin/security"
                task.arguments = ["find-generic-password", "-l", "BeaconStore", "-g"]

                log("CMD: /usr/bin/security find-generic-password -l BeaconStore -g")

                let pipe = Pipe()
                task.standardOutput = pipe
                task.launch()

                let data = pipe.fileHandleForReading.readDataToEndOfFile()

                if let result = String(data: data, encoding: .utf8) {
                    log("RESULT: \(result)")
                    var gena = result.components(separatedBy: "\"gena\"<blob>=")

                    if gena.count > 1 {
                        gena = gena[1].components(separatedBy: "  \"")

                        if gena.count > 0 {
                            if gena[0].count > 0 {
                                var genaHexString = gena[0]
                                
                                let data = getKeyData(from: genaHexString)

                                keyData = Data(data)
                            }
                        }
                    }
                }
            }

            if keyData.count == 0 {
                log("Error: Cannot get Beacon key data.\nPlease try to run the below command on your Terminal, and save the \"gena\" value (it looks like \"0xABCDEF...7890\" without spaces) to the Extras panel.")
                log("/usr/bin/security find-generic-password -l BeaconStore -g")
            } else {
                log("Got Beacon key data (\(keyData.count) byte(s))")

                let key = SymmetricKey(data: keyData)

                func decryptRecord(url: URL, key: SymmetricKey) throws -> [String: Any]? {
                    do {
                        let data = try Data(contentsOf: url)

                        if let plist = try PropertyListSerialization.propertyList(
                            from: data,
                            options: [],
                            format: nil
                        ) as? [Any] {
                            if plist.count >= 3,
                                let nonceData = plist[0] as? Data,
                                let tagData = plist[1] as? Data,
                                let ciphertextData = plist[2] as? Data
                            {

                                let sealedBox = try AES.GCM.SealedBox(
                                    nonce: AES.GCM.Nonce(data: nonceData),
                                    ciphertext: ciphertextData,
                                    tag: tagData
                                )

                                let decryptedData = try AES.GCM.open(sealedBox, using: key)

                                if let decryptedPlist = try PropertyListSerialization.propertyList(
                                    from: decryptedData,
                                    options: [],
                                    format: nil
                                ) as? [String: Any] {
                                    return decryptedPlist
                                }

                            }

                        }
                    } catch {
                        print("Decrypt \(url) error:", error)
                    }

                    return nil
                }

                let searchpartydUrl = FileManager.default.urls(
                    for: .libraryDirectory, in: .userDomainMask
                ).first!.appendingPathComponent("com.apple.icloud.searchpartyd")

                let beaconNamingRecordUrl = searchpartydUrl.appendingPathComponent(
                    "BeaconNamingRecord")
                let beaconEstimatedLocationUrl = searchpartydUrl.appendingPathComponent(
                    "BeaconEstimatedLocation")
                let sharedBeaconsUrl = searchpartydUrl.appendingPathComponent("OwnedBeacons")

                let fileManager = FileManager.default

                var beaconNames = [String: String]()
                var sharedBeaconMap = [String: String]()
                var beacons = [String: Beacon]()

                do {
                    if fileManager.fileExists(atPath: beaconNamingRecordUrl.path) {
                        log("Scanning beacon names...")
                        
                        let namingRecords = try fileManager.contentsOfDirectory(
                            at: beaconNamingRecordUrl, includingPropertiesForKeys: nil)
                        for subDir in namingRecords {
                            if subDir.lastPathComponent != ".DS_Store" {
                                let namingItems = try fileManager.contentsOfDirectory(
                                    at: beaconNamingRecordUrl.appendingPathComponent(
                                        subDir.lastPathComponent), includingPropertiesForKeys: nil)
                                for namingItem in namingItems {
                                    if let decryptedData = try decryptRecord(
                                        url: namingItem, key: key)
                                    {

                                        if let identifier = decryptedData["associatedBeacon"]
                                            as? String,
                                            let name = decryptedData["name"] as? String
                                        {
                                            beaconNames[identifier] = name
                                        }
                                    } else {
                                        log("Error: Cannot decrypt beacon name record \(namingItem.lastPathComponent)")
                                    }
                                }
                            }
                        }
                    }
                    
                    if fileManager.fileExists(atPath: sharedBeaconsUrl.path) {
                        log("Scanning shared beacons...")
                        let sharedRecords = try fileManager.contentsOfDirectory(
                            at: sharedBeaconsUrl, includingPropertiesForKeys: nil)
                        for sharedRecord in sharedRecords {
                            if let decryptedData = try decryptRecord(
                                url: sharedRecord, key: key)
                            {
                                if let identifier = decryptedData["identifier"] as? String,
                                   let name = decryptedData["name"] as? [String: Any]
                                {
                                    if let ownerBeaconIdentifier = name["ownerBeaconIdentifier"]
                                        as? String
                                    {
                                        sharedBeaconMap[identifier] = ownerBeaconIdentifier
                                    }
                                }
                            } else {
                                log("Error: Cannot decrypt shared beacon record \(sharedRecord.lastPathComponent)")
                            }
                        }
                    }

                    if fileManager.fileExists(atPath: beaconEstimatedLocationUrl.path) {
                        log("Scanning beacon locations...")
                        
                        let beaconEstimatedLocations = try fileManager.contentsOfDirectory(
                            at: beaconEstimatedLocationUrl, includingPropertiesForKeys: nil)
                        for subDir in beaconEstimatedLocations {
                            if subDir.lastPathComponent != ".DS_Store" {
                                let locationItems = try fileManager.contentsOfDirectory(
                                    at: beaconEstimatedLocationUrl.appendingPathComponent(
                                        subDir.lastPathComponent), includingPropertiesForKeys: nil)
                                for locationItem in locationItems {
                                    if let decryptedData = try decryptRecord(
                                        url: locationItem, key: key)
                                    {
                                        
                                        if let identifier = decryptedData["associatedBeacon"]
                                            as? String,
                                           let latitude = decryptedData["latitude"],
                                           let longitude = decryptedData["longitude"],
                                           let timestamp = decryptedData["timestamp"],
                                           let horizontalAccuracy = decryptedData["horizontalAccuracy"]
                                        {
                                            var id = identifier
                                            
                                            if let mapIdentifier = sharedBeaconMap[identifier] {
                                                id = mapIdentifier
                                            }
                                            
                                            var beacon = Beacon(
                                                identifier: id,
                                                name: id,
                                                accuracy: NSNumber(
                                                    value: horizontalAccuracy as! Double),
                                                longitude: NSNumber(value: longitude as! Double),
                                                latitude: NSNumber(value: latitude as! Double)
                                            )
                                            
                                            if let timestamp = timestamp as? Date {
                                                beacon.timestamp = timestamp
                                            }
                                            
                                            if let name = beaconNames[id] {
                                                beacon.name = name
                                            }
                                            
                                            if let existedBeacon = beacons[id] {
                                                if let existedBeaconTimestamp = existedBeacon.timestamp
                                                {
                                                    if let beaconTimestamp = beacon.timestamp {
                                                        if beaconTimestamp > existedBeaconTimestamp {
                                                            beacons[id] = beacon
                                                        }
                                                    }
                                                } else {
                                                    beacons[id] = beacon
                                                }
                                            } else {
                                                beacons[id] = beacon
                                            }
                                            
                                        }
                                    } else {
                                        log("Error: Cannot decrypt beacon location record \(locationItem.lastPathComponent)")
                                    }
                                }
                            }
                        }
                    }

                    log("Scanned \(beacons.count) records")
                    readFindMy = true

                    for (identifier, beacon) in beacons {
                        print(identifier, beacon)
                        updateEntity(
                            id: identifier,
                            name: beacon.name,
                            latitude: beacon.latitude,
                            longitude:
                                beacon.longitude,
                            accuracy: beacon.accuracy,
                            battery: -1,
                            address: "",
                            timestamp: beacon.timestamp
                        )
                        publishedCount += 1

                        if generate_config {
                            haConfig += "device_tracker.\(MQTTPublisher.entityId(for: identifier))  # \(beacon.name)\n"
                        }

                        log(

                            "- Item \"" + beacon.name + "\" - " + identifier + "\n"
                                + "- - Location: "
                                + beacon.latitude
                                .stringValue
                                + ", "
                                + beacon.longitude
                                .stringValue
                                + " (Accuracy "
                                + beacon.accuracy
                                .stringValue
                                + ")")
                    }
                } catch {
                    log("Cannot parse beacon record: " + error.localizedDescription)
                    hasAccess = false
                }
            }
        } else {
            let items: Bool = UserDefaults.standard.bool(forKey: "sources_items")
            let devices: Bool = UserDefaults.standard.bool(forKey: "sources_devices")

            if items {
                let itemsJsonFile = homeDirectory.appendingPathComponent(
                    "Library/Caches/com.apple.findmy.fmipcore/Items.data")

                log("Items file: " + itemsJsonFile.absoluteString)

                do {
                    let data = try Data(
                        contentsOf: itemsJsonFile, options: .mappedIfSafe)
                    let jsonResult = try JSONSerialization.jsonObject(
                        with: data, options: .mutableLeaves)
                    if let jsonResult = jsonResult as? [AnyObject] {
                        log("Got " + String(jsonResult.count) + " item(s):")
                        readFindMy = true
                        for item in jsonResult {
                            if let id = item["identifier"] as? String {
                                let name = item["name"] as! String
                                log("- Item \"" + name + "\" - " + id)
                                if let location = item["location"]
                                    as? [String: Any]
                                {
                                    if let latitude =
                                        location["latitude"]
                                        as? NSNumber,
                                        let longitude =
                                            location[
                                                "longitude"]
                                            as? NSNumber,
                                        let accuracy =
                                            location[
                                                "horizontalAccuracy"
                                            ] as? NSNumber
                                    {
                                        log(
                                            "- - Location: "
                                                + latitude
                                                .stringValue
                                                + ", "
                                                + longitude
                                                .stringValue
                                                + " (Accuracy "
                                                + accuracy
                                                .stringValue
                                                + ")")

                                        var fullAddress = ""
                                        if let address =
                                            item["address"]
                                            as? [String: Any]
                                        {
                                            if let
                                                mapItemFullAddress =
                                                address[
                                                    "mapItemFullAddress"
                                                ] as? String
                                            {
                                                fullAddress =
                                                    mapItemFullAddress
                                            } else if let
                                                label =
                                                address[
                                                    "label"
                                                ] as? String
                                            {
                                                fullAddress =
                                                    label
                                            } else if let
                                                fullThroroughfare =
                                                address[
                                                    "fullThroroughfare"
                                                ] as? String
                                            {
                                                fullAddress =
                                                    fullThroroughfare
                                            } else if let
                                                streetName =
                                                address[
                                                    "streetName"
                                                ] as? String
                                            {
                                                fullAddress =
                                                    streetName
                                            } else if let
                                                locality =
                                                address[
                                                    "locality"
                                                ] as? String
                                            {
                                                fullAddress =
                                                    locality
                                            }
                                        }

                                        updateEntity(
                                            id: id,
                                            name: name,
                                            latitude: latitude,
                                            longitude:
                                                longitude,
                                            accuracy: accuracy,
                                            battery: -1,
                                            address: fullAddress,
                                            timestamp: Synchronizer.fixTime(from: location)
                                        )
                                        publishedCount += 1

                                        if generate_config {
                                            haConfig += "device_tracker.\(MQTTPublisher.entityId(for: id))  # \(name)\n"
                                        }
                                    } else {
                                        log(
                                            "- - No latitude/longitude/accuracy provided"
                                        )
                                    }
                                } else {
                                    log("- - No location provided")
                                }
                            }
                        }
                    }
                } catch {
                    log("Cannot read Items file: " + error.localizedDescription)
                    hasAccess = false
                }
            }

            if devices {
                let devicesJsonFile = homeDirectory.appendingPathComponent(
                    "Library/Caches/com.apple.findmy.fmipcore/Devices.data")
                log("Devices file: " + devicesJsonFile.absoluteString)

                do {
                    let data = try Data(
                        contentsOf: devicesJsonFile, options: .mappedIfSafe)
                    let jsonResult = try JSONSerialization.jsonObject(
                        with: data, options: .mutableLeaves)
                    if let jsonResult = jsonResult as? [AnyObject] {
                        log("Got " + String(jsonResult.count) + " item(s):")
                        readFindMy = true
                        for item in jsonResult {
                            if let id = item["baUUID"] as? String {
                                let name = item["name"] as! String
                                log("- Item \"" + name + "\" - " + id)
                                if let location = item["location"]
                                    as? [String: Any]
                                {
                                    if let latitude =
                                        location["latitude"]
                                        as? NSNumber,
                                        let longitude =
                                            location[
                                                "longitude"]
                                            as? NSNumber,
                                        let accuracy =
                                            location[
                                                "horizontalAccuracy"
                                            ] as? NSNumber
                                    {
                                        log(
                                            "- - Location: "
                                                + latitude
                                                .stringValue
                                                + ", "
                                                + longitude
                                                .stringValue
                                                + " (Accuracy "
                                                + accuracy
                                                .stringValue
                                                + ")")

                                        var fullAddress = ""
                                        if let address =
                                            item["address"]
                                            as? [String: Any]
                                        {
                                            if let
                                                mapItemFullAddress =
                                                address[
                                                    "mapItemFullAddress"
                                                ] as? String
                                            {
                                                fullAddress =
                                                    mapItemFullAddress
                                            } else if let
                                                label =
                                                address[
                                                    "label"
                                                ] as? String
                                            {
                                                fullAddress =
                                                    label
                                            } else if let
                                                fullThroroughfare =
                                                address[
                                                    "fullThroroughfare"
                                                ] as? String
                                            {
                                                fullAddress =
                                                    fullThroroughfare
                                            } else if let
                                                streetName =
                                                address[
                                                    "streetName"
                                                ] as? String
                                            {
                                                fullAddress =
                                                    streetName
                                            } else if let
                                                locality =
                                                address[
                                                    "locality"
                                                ] as? String
                                            {
                                                fullAddress =
                                                    locality
                                            }
                                        }

                                        var batteryLevel = NSNumber(
                                            value: -1)

                                        if let battery =
                                            item["batteryLevel"]
                                            as? NSNumber
                                        {
                                            log(
                                                "- - Battery level: "
                                                    + battery
                                                    .stringValue
                                            )
                                            batteryLevel =
                                                battery
                                        }

                                        updateEntity(
                                            id: id,
                                            name: name,
                                            latitude: latitude,
                                            longitude:
                                                longitude,
                                            accuracy: accuracy,
                                            battery:
                                                batteryLevel,
                                            address: fullAddress,
                                            timestamp: Synchronizer.fixTime(from: location)
                                        )
                                        publishedCount += 1

                                        if generate_config {
                                            haConfig += "device_tracker.\(MQTTPublisher.entityId(for: id))  # \(name)\n"
                                        }
                                    } else {
                                        log(
                                            "- - No latitude/longitude/accuracy provided"
                                        )
                                    }
                                } else {
                                    log("- - No location provided")
                                }
                            }
                        }
                    }
                } catch {
                    log("Cannot read Items file: " + error.localizedDescription)
                    hasAccess = false
                }
            }
        }

        if !hasAccess {
            if retried < 1 {
                retried += 1
                log("Cannot access FindMy Data, will retry")
            } else {
                onAccessDenied()
            }
        } else {
            retried = 0
            if generate_config {
                logConfig(haConfig)
            }
            // Heartbeat only after a pass that actually read Find My. A pass
            // that could not read the cache publishes nothing, so the
            // `sensor.findmy_last_sync` timestamp goes stale — which is the point.
            if readFindMy {
                MQTTPublisher.shared.finish(deviceCount: publishedCount)
            }
        }

        // Clamped read, so a stray value written by hand or over MQTT cannot
        // schedule a zero-second timer. The previous force-unwrapped
        // TimeInterval(interval)! also crashed on any non-numeric string.
        let interval = MQTTPublisher.currentInterval()
        log("Scheduling next update after \(interval) minutes")

        if let currentTimer = timer {
            currentTimer.invalidate()
        }
        self.timer = Timer.scheduledTimer(
            withTimeInterval: TimeInterval(interval) * 60, repeats: false
        ) { t in
            self.fetchData()
        }
    }

    func updateEntity(
        id: String, name: String, latitude: NSNumber, longitude: NSNumber, accuracy: NSNumber,
        battery: NSNumber, address: String, timestamp: Date? = nil
    ) {
        MQTTPublisher.shared.publish(
            identifier: id,
            name: name,
            latitude: latitude,
            longitude: longitude,
            accuracy: accuracy,
            battery: battery,
            address: address,
            timestamp: timestamp
        )
    }

    /// Find My's cache files carry the fix time as `timeStamp`, milliseconds
    /// since the epoch. Older dumps have been seen with seconds and with the
    /// key in lower case, so accept both; anything unparseable is `nil` and the
    /// attribute is simply omitted.
    static func fixTime(from location: [String: Any]) -> Date? {
        let raw = location["timeStamp"] ?? location["timestamp"]
        if let date = raw as? Date { return date }
        guard let number = raw as? NSNumber else { return nil }
        let value = number.doubleValue
        guard value > 0 else { return nil }
        // Anything past the year 2100 in seconds is milliseconds.
        return Date(timeIntervalSince1970: value > 4_102_444_800 ? value / 1000 : value)
    }
}
