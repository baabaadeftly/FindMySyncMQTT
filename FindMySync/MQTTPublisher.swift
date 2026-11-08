//
//  MQTTPublisher.swift
//  FindMySync
//
//  Publishes Find My locations to Home Assistant over MQTT using MQTT
//  discovery, replacing the deprecated device_tracker.see action
//  (removed in Home Assistant Core 2027.5).
//
//  Two retained topics per device:
//
//    <discovery_prefix>/device_tracker/findmy_<id>/config
//    <topic_prefix>/findmy_<id>/attributes
//
//  The discovery message deliberately omits `state_topic`. When a device
//  tracker has no state topic but its json_attributes carry latitude,
//  longitude and gps_accuracy, Home Assistant resolves the zone itself and
//  sets home / not_home / <zone name>. That reproduces what device_tracker.see
//  did. Publishing a state topic here would override that with a literal
//  string and break zone detection.
//
//  Both topics are retained, so the last known position is republished to
//  Home Assistant the moment it reconnects — the state survives a restart,
//  which template device trackers do not.
//
//  Two control entities are also published, grouped under a single
//  "FindMySync" device in the Home Assistant device registry:
//
//    <discovery_prefix>/number/findmysync_update_interval/config
//    <discovery_prefix>/button/findmysync_sync_now/config
//
//  and the app subscribes to their command topics:
//
//    <topic_prefix>/interval/set     minutes, 1–60
//    <topic_prefix>/sync/set         any payload triggers a sync pass
//
//  The interval command writes the `extra_interval` default — the same one
//  the Extras pane writes — and reschedules the sync timer, so the poll rate
//  becomes controllable from Home Assistant and from automations.
//

import CocoaMQTT
import Foundation

final class MQTTPublisher {

    static let shared = MQTTPublisher()

    /// Assigned by Synchronizer so broker activity appears in the Status pane.
    var log: (_ message: String) -> Void = { debugPrint($0) }

    /// Bounds for the sync interval, in minutes. Mirrored in the `number`
    /// discovery payload so Home Assistant clamps before we have to.
    static let minIntervalMinutes = 1
    static let maxIntervalMinutes = 60

    private let queue = DispatchQueue(label: "com.findmysync.mqtt")
    private var client: CocoaMQTT?
    private var connectionSignature = ""
    private var delegateShim: DelegateShim?

    /// Consecutive sync passes that ended without a connected client. Once it
    /// reaches `rebuildAfterFailedPasses` the client is thrown away and rebuilt,
    /// because CocoaMQTT's auto-reconnect has been seen to stall for days in
    /// `.connecting` after the broker became unreachable, and never recover once
    /// it was reachable again. A fresh socket does recover.
    private var failedPasses = 0
    private static let rebuildAfterFailedPasses = 2

    /// TCP connect timeout. The default `connect()` has none and leaves the
    /// socket waiting on the OS, which is where the stall above began.
    private static let connectTimeout: TimeInterval = 10

    private static let iso8601: ISO8601DateFormatter = {
        let f = ISO8601DateFormatter()
        f.formatOptions = [.withInternetDateTime]
        return f
    }()

    /// entity ID -> the name last announced for it. Discovery is republished
    /// only when a device is new or has been renamed in Find My.
    private var announced = [String: String]()

    /// Control discovery is published once per connection, not per sync pass.
    private var controlsAnnounced = false

    private init() {}

    // MARK: - Entity naming

    /// Entity IDs must stay stable across the migration. `binary_sensor.findmy_feed_stale`
    /// matches on `^device_tracker\.findmy_`, and these are the same IDs
    /// device_tracker.see produced from known_devices.yaml.
    static func entityId(for identifier: String) -> String {
        "findmy_" + identifier.replacingOccurrences(of: "-", with: "")
    }

    // MARK: - Interval

    /// Reads `extra_interval`, clamped. The Extras pane writes the same key.
    static func currentInterval() -> Int {
        let raw = Int(UserDefaults.standard.string(forKey: "extra_interval") ?? "5") ?? 5
        return min(max(raw, minIntervalMinutes), maxIntervalMinutes)
    }

    // MARK: - Settings

    private struct Settings {
        var host = ""
        var port: UInt16 = 1883
        var username = ""
        var password = ""
        var discoveryPrefix = "homeassistant"
        var topicPrefix = "findmysync"
        var publishAvailability = false

        /// Changing any of these forces a reconnect.
        var signature: String {
            [host, String(port), username, password, discoveryPrefix, topicPrefix]
                .joined(separator: "|")
        }

        var statusTopic: String {
            "\(topicPrefix)/status"
        }

        var intervalCommandTopic: String {
            "\(topicPrefix)/interval/set"
        }

        var intervalStateTopic: String {
            "\(topicPrefix)/interval/state"
        }

        var syncCommandTopic: String {
            "\(topicPrefix)/sync/set"
        }

        var lastSyncTopic: String {
            "\(topicPrefix)/status/last_sync"
        }

        func attributesTopic(_ entityId: String) -> String {
            "\(topicPrefix)/\(entityId)/attributes"
        }

        func discoveryTopic(_ entityId: String) -> String {
            "\(discoveryPrefix)/device_tracker/\(entityId)/config"
        }

        func controlDiscoveryTopic(component: String, objectId: String) -> String {
            "\(discoveryPrefix)/\(component)/\(objectId)/config"
        }
    }

    private func loadSettings() -> Settings {
        let defaults = UserDefaults.standard
        var settings = Settings()

        settings.host = (defaults.string(forKey: "mqtt_host") ?? "")
            .trimmingCharacters(in: .whitespacesAndNewlines)
        settings.port = UInt16(defaults.string(forKey: "mqtt_port") ?? "1883") ?? 1883
        settings.username = defaults.string(forKey: "mqtt_username") ?? ""
        settings.password = defaults.string(forKey: "mqtt_password") ?? ""
        settings.discoveryPrefix = MQTTPublisher.cleanPrefix(
            defaults.string(forKey: "mqtt_discovery_prefix"), fallback: "homeassistant")
        settings.topicPrefix = MQTTPublisher.cleanPrefix(
            defaults.string(forKey: "mqtt_topic_prefix"), fallback: "findmysync")
        settings.publishAvailability = defaults.bool(forKey: "mqtt_availability")

        return settings
    }

    private static func cleanPrefix(_ value: String?, fallback: String) -> String {
        let trimmed =
            (value ?? "")
            .trimmingCharacters(in: .whitespacesAndNewlines)
            .trimmingCharacters(in: CharacterSet(charactersIn: "/"))
        return trimmed.isEmpty ? fallback : trimmed
    }

    // MARK: - Public API

    /// Called once at the start of every sync pass, before any publish.
    /// Everything runs on one serial queue, so this always completes first.
    func begin() {
        queue.async {
            let settings = self.loadSettings()

            guard !settings.host.isEmpty else {
                self.log("MQTT: no broker host configured — nothing will be published")
                return
            }

            if self.ensureConnected(settings) {
                self.failedPasses = 0
                self.log("MQTT: connected to \(settings.host):\(settings.port)")
                if settings.publishAvailability {
                    self.send(topic: settings.statusTopic, payload: "online")
                }
                self.announceControlsIfNeeded(settings)
            } else {
                self.failedPasses += 1
                let state = self.client.map { "\($0.connState)" } ?? "no client"
                self.log(
                    "MQTT: \(settings.host):\(settings.port) not reachable "
                        + "(state \(state), \(self.failedPasses) failed pass"
                        + (self.failedPasses == 1 ? "" : "es")
                        + ") — messages are queued and flush on connect")
            }
        }
    }

    /// Called once at the end of every sync pass that read Find My successfully.
    /// Publishes the heartbeat Home Assistant uses to tell a live feed from a
    /// retained replay: the topic is retained like everything else, but a
    /// replay carries the *old* `last_sync`, so a sensor keyed on it stays stale
    /// through a Home Assistant restart while the app is not publishing.
    func finish(deviceCount: Int) {
        queue.async {
            let settings = self.loadSettings()
            guard !settings.host.isEmpty else { return }

            let payload: [String: Any] = [
                "last_sync": MQTTPublisher.iso8601.string(from: Date()),
                "device_count": deviceCount,
            ]
            guard let json = MQTTPublisher.json(payload) else { return }
            self.send(topic: settings.lastSyncTopic, payload: json)
            self.log("MQTT: heartbeat published (\(deviceCount) devices)")
        }
    }

    func publish(
        identifier: String, name: String, latitude: NSNumber, longitude: NSNumber,
        accuracy: NSNumber, battery: NSNumber, address: String, timestamp: Date? = nil
    ) {
        queue.async {
            let settings = self.loadSettings()
            guard !settings.host.isEmpty else { return }

            _ = self.ensureConnected(settings)

            let entityId = MQTTPublisher.entityId(for: identifier)
            let displayName = name.isEmpty ? entityId : name

            if self.announced[entityId] != displayName {
                self.sendDiscovery(entityId: entityId, name: displayName, settings: settings)
                self.announced[entityId] = displayName
            }

            // `address` is deliberately not published. Apple's reverse geocoding
            // flips between neighbouring street numbers on a stationary device,
            // which forced a recorder write on every poll. The attributes topic
            // is retained and replaced whole, so dropping the key here removes
            // it from the entity on the next publish — no cleanup needed.
            var attributes: [String: Any] = [
                "latitude": latitude.doubleValue,
                "longitude": longitude.doubleValue,
                "gps_accuracy": accuracy.doubleValue,
            ]

            // Find My reports battery as a 0–1 fraction, and -1 when unknown.
            if battery.doubleValue > 0 {
                attributes["battery_level"] = Int((battery.doubleValue * 100).rounded())
            }

            // Find My's own fix time for the device, ISO 8601 UTC. It only
            // advances when Find My has a new fix, so it costs no extra recorder
            // writes on a stationary device, and it does not advance while Find
            // My itself is stale (window minimised, signed out) — which the
            // coordinates alone cannot show. The per-pass heartbeat lives on a
            // separate topic (see `finish`), deliberately not here: a timestamp
            // that changes every pass on every device would force a recorder
            // write per device per poll, the same cost that got `address` removed.
            if let timestamp = timestamp {
                attributes["last_seen"] = MQTTPublisher.iso8601.string(from: timestamp)
            }

            guard let payload = MQTTPublisher.json(attributes) else {
                self.log("[\(identifier)] MQTT: could not encode attributes")
                return
            }

            self.send(topic: settings.attributesTopic(entityId), payload: payload)
            self.log("[\(identifier)] MQTT: published to \(settings.attributesTopic(entityId))")
        }
    }

    // MARK: - Publishing

    private func sendDiscovery(entityId: String, name: String, settings: Settings) {
        // `default_entity_id` is what pins the entity ID. `object_id` is ignored by
        // Home Assistant 2026.8 — verified against the live instance, where a discovery
        // payload carrying object_id "findmy_testb" still landed as
        // device_tracker.mqtt_test_b, derived from `name`. `default_entity_id` takes the
        // full entity ID including the domain.
        var config: [String: Any] = [
            "name": name,
            "unique_id": entityId,
            "default_entity_id": "device_tracker.\(entityId)",
            "json_attributes_topic": settings.attributesTopic(entityId),
            "source_type": "gps",
        ]

        if settings.publishAvailability {
            config["availability_topic"] = settings.statusTopic
            config["payload_available"] = "online"
            config["payload_not_available"] = "offline"
        }

        guard let payload = MQTTPublisher.json(config) else {
            log("[\(entityId)] MQTT: could not encode discovery config")
            return
        }

        send(topic: settings.discoveryTopic(entityId), payload: payload)
        log("[\(entityId)] MQTT: discovery published")
    }

    // MARK: - Control entities

    /// The trackers stay entity-only — they were migrated from known_devices.yaml
    /// and attaching them to a device now would risk their entity IDs. The two
    /// controls are new, so they get a device block and group together in the
    /// registry as "FindMySync".
    private func deviceBlock() -> [String: Any] {
        let version =
            Bundle.main.object(forInfoDictionaryKey: "CFBundleShortVersionString") as? String
            ?? "unknown"
        return [
            "identifiers": ["findmysync"],
            "name": "FindMySync",
            "manufacturer": "FindMySync",
            "model": "Find My bridge (MQTT)",
            "sw_version": version,
        ]
    }

    private func announceControlsIfNeeded(_ settings: Settings) {
        guard !controlsAnnounced else { return }

        let device = deviceBlock()

        var number: [String: Any] = [
            "name": "Update interval",
            "unique_id": "findmysync_update_interval",
            "default_entity_id": "number.findmy_update_interval",
            "command_topic": settings.intervalCommandTopic,
            "state_topic": settings.intervalStateTopic,
            "min": MQTTPublisher.minIntervalMinutes,
            "max": MQTTPublisher.maxIntervalMinutes,
            "step": 1,
            "mode": "slider",
            "unit_of_measurement": "min",
            "icon": "mdi:timer-sync-outline",
            "entity_category": "config",
            // Retained commands mean the app picks the last value back up on
            // restart even if it was offline when Home Assistant sent it.
            "retain": true,
            "device": device,
        ]

        var button: [String: Any] = [
            "name": "Sync now",
            "unique_id": "findmysync_sync_now",
            "default_entity_id": "button.findmy_sync_now",
            "command_topic": settings.syncCommandTopic,
            "payload_press": "PRESS",
            "icon": "mdi:refresh",
            "device": device,
        ]

        // No availability on this one on purpose: its whole job is to keep
        // reporting the old timestamp when the app is gone.
        let lastSync: [String: Any] = [
            "name": "Last sync",
            "unique_id": "findmysync_last_sync",
            "default_entity_id": "sensor.findmy_last_sync",
            "state_topic": settings.lastSyncTopic,
            "value_template": "{{ value_json.last_sync }}",
            "json_attributes_topic": settings.lastSyncTopic,
            "device_class": "timestamp",
            "icon": "mdi:clock-check-outline",
            "entity_category": "diagnostic",
            "device": device,
        ]

        if settings.publishAvailability {
            number["availability_topic"] = settings.statusTopic
            number["payload_available"] = "online"
            number["payload_not_available"] = "offline"
            button["availability_topic"] = settings.statusTopic
            button["payload_available"] = "online"
            button["payload_not_available"] = "offline"
        }

        if let payload = MQTTPublisher.json(lastSync) {
            send(
                topic: settings.controlDiscoveryTopic(
                    component: "sensor", objectId: "findmysync_last_sync"),
                payload: payload)
        }

        if let payload = MQTTPublisher.json(number) {
            send(
                topic: settings.controlDiscoveryTopic(
                    component: "number", objectId: "findmysync_update_interval"),
                payload: payload)
        }

        if let payload = MQTTPublisher.json(button) {
            send(
                topic: settings.controlDiscoveryTopic(
                    component: "button", objectId: "findmysync_sync_now"),
                payload: payload)
        }

        send(
            topic: settings.intervalStateTopic,
            payload: String(MQTTPublisher.currentInterval()))

        controlsAnnounced = true
        log("MQTT: control entities published (update interval, sync now, last sync)")
    }

    private func subscribeToCommands(_ settings: Settings) {
        guard let client = client else { return }
        // cleanSession is true, so subscriptions do not survive a reconnect.
        // This is called from didConnectAck on every connection, not just the first.
        client.subscribe([
            (settings.intervalCommandTopic, CocoaMQTTQoS.qos1),
            (settings.syncCommandTopic, CocoaMQTTQoS.qos1),
        ])
    }

    // MARK: - Command handling

    fileprivate func handleIncoming(topic: String, payload: String) {
        queue.async {
            let settings = self.loadSettings()

            if topic == settings.intervalCommandTopic {
                let trimmed = payload.trimmingCharacters(in: .whitespacesAndNewlines)
                guard let asDouble = Double(trimmed) else {
                    self.log("MQTT: ignoring unparseable interval '\(trimmed)'")
                    return
                }

                let requested = Int(asDouble.rounded())
                let clamped = min(
                    max(requested, MQTTPublisher.minIntervalMinutes),
                    MQTTPublisher.maxIntervalMinutes)

                if clamped != requested {
                    self.log(
                        "MQTT: interval \(requested) out of range, clamped to \(clamped) minutes")
                }

                let previous = MQTTPublisher.currentInterval()
                UserDefaults.standard.set(String(clamped), forKey: "extra_interval")
                self.send(topic: settings.intervalStateTopic, payload: String(clamped))

                guard clamped != previous else {
                    // A retained command replayed on reconnect, or a no-op set.
                    // Echoing state is enough; re-running the sync pass is not.
                    return
                }

                self.log("MQTT: update interval set to \(clamped) minutes")
                // fetchData reschedules the timer at the end of the pass, and
                // Timer.scheduledTimer needs the main run loop.
                DispatchQueue.main.async { Synchronizer.shared.fetchData() }

            } else if topic == settings.syncCommandTopic {
                self.log("MQTT: sync requested from Home Assistant")
                DispatchQueue.main.async { Synchronizer.shared.fetchData() }
            }
        }
    }

    fileprivate func handleConnected() {
        queue.async {
            let settings = self.loadSettings()
            guard !settings.host.isEmpty else { return }

            self.subscribeToCommands(settings)

            if settings.publishAvailability {
                self.send(topic: settings.statusTopic, payload: "online")
            }

            self.announceControlsIfNeeded(settings)
        }
    }

    private func send(topic: String, payload: String) {
        guard let client = client else { return }
        _ = client.publish(topic, withString: payload, qos: .qos1, retained: true)
    }

    private static func json(_ object: [String: Any]) -> String? {
        guard let data = try? JSONSerialization.data(withJSONObject: object, options: [])
        else {
            return nil
        }
        return String(data: data, encoding: .utf8)
    }

    // MARK: - Connection

    private func ensureConnected(_ settings: Settings) -> Bool {
        if let existing = client, connectionSignature == settings.signature,
            existing.connState == .connected
        {
            return true
        }

        // Tear the connection down and re-announce everything when the settings
        // changed, or when the existing client has failed to connect for
        // `rebuildAfterFailedPasses` passes in a row. The second case is the
        // one that matters: a client that has gone through the broker being
        // unreachable can stay in `.connecting` indefinitely, with
        // autoReconnect no longer firing, and `connect()` on it is a no-op.
        // Observed 6–8 Nov 2026: two days of "not reachable" against a broker
        // that was reachable again, cured only by relaunching the app.
        let settingsChanged = connectionSignature != settings.signature
        let stuck = failedPasses >= MQTTPublisher.rebuildAfterFailedPasses
        if settingsChanged || stuck, let existing = client {
            if stuck {
                log("MQTT: connection stuck in \(existing.connState) — rebuilding the client")
            }
            // Stop the old client's own retry timer first; otherwise it can
            // reconnect later with the same clientID and bounce the new one.
            existing.autoReconnect = false
            existing.delegate = nil
            existing.disconnect()
            client = nil
            delegateShim = nil
            announced.removeAll()
            controlsAnnounced = false
            failedPasses = 0
        }

        if client == nil {
            let suffix =
                ProcessInfo.processInfo.hostName
                .replacingOccurrences(of: ".", with: "-")
                .replacingOccurrences(of: " ", with: "-")

            let mqtt = CocoaMQTT(
                clientID: "findmysync-" + suffix, host: settings.host, port: settings.port)

            if !settings.username.isEmpty { mqtt.username = settings.username }
            if !settings.password.isEmpty { mqtt.password = settings.password }
            mqtt.keepAlive = 60
            mqtt.cleanSession = true
            mqtt.autoReconnect = true
            // Default is 128 s of exponential backoff; with a sync pass every
            // 1–60 min the rebuild above is the real recovery path, so cap the
            // library's own retry at something that still beats the next pass.
            mqtt.maxAutoReconnectTimeInterval = 30

            if settings.publishAvailability {
                let will = CocoaMQTTMessage(topic: settings.statusTopic, string: "offline")
                will.qos = .qos1
                will.retained = true
                mqtt.willMessage = will
            }

            let shim = DelegateShim(owner: self)
            delegateShim = shim
            mqtt.delegate = shim

            client = mqtt
            connectionSignature = settings.signature
            _ = mqtt.connect(timeout: MQTTPublisher.connectTimeout)
        } else if let existing = client, existing.connState == .disconnected {
            // Only kick a client that is actually idle. Calling connect() on one
            // that is mid-handshake throws inside the socket layer and leaves it
            // where it was; the failed-pass counter deals with that case.
            _ = existing.connect(timeout: MQTTPublisher.connectTimeout)
        }

        // Bounded wait. This runs on our own serial queue, never the main thread.
        // Publishing before the handshake completes is safe anyway — CocoaMQTT
        // queues messages internally — but waiting makes the Status log honest.
        let deadline = Date().addingTimeInterval(5)
        while Date() < deadline {
            if client?.connState == .connected {
                return true
            }
            Thread.sleep(forTimeInterval: 0.1)
        }

        return client?.connState == .connected
    }
}

// MARK: - Delegate

/// CocoaMQTTDelegate is an @objc protocol, so the conformer has to be an
/// NSObject. Keeping it in a shim rather than on MQTTPublisher itself avoids
/// exposing nine delegate methods on the publisher's own surface.
private final class DelegateShim: NSObject, CocoaMQTTDelegate {

    private unowned let owner: MQTTPublisher

    init(owner: MQTTPublisher) {
        self.owner = owner
        super.init()
    }

    func mqtt(_ mqtt: CocoaMQTT, didConnectAck ack: CocoaMQTTConnAck) {
        guard ack == .accept else {
            owner.log("MQTT: broker refused the connection (\(ack))")
            return
        }
        owner.handleConnected()
    }

    func mqtt(_ mqtt: CocoaMQTT, didReceiveMessage message: CocoaMQTTMessage, id: UInt16) {
        guard let payload = message.string else { return }
        owner.handleIncoming(topic: message.topic, payload: payload)
    }

    func mqtt(_ mqtt: CocoaMQTT, didSubscribeTopics success: NSDictionary, failed: [String]) {
        if !failed.isEmpty {
            owner.log("MQTT: failed to subscribe to \(failed.joined(separator: ", "))")
        }
    }

    func mqttDidDisconnect(_ mqtt: CocoaMQTT, withError err: Error?) {
        if let err = err {
            owner.log("MQTT: disconnected — \(err.localizedDescription)")
        } else {
            owner.log("MQTT: disconnected")
        }
    }

    // Not used, but required by the protocol.
    func mqtt(_ mqtt: CocoaMQTT, didPublishMessage message: CocoaMQTTMessage, id: UInt16) {}
    func mqtt(_ mqtt: CocoaMQTT, didPublishAck id: UInt16) {}
    func mqtt(_ mqtt: CocoaMQTT, didUnsubscribeTopics topics: [String]) {}
    func mqttDidPing(_ mqtt: CocoaMQTT) {}
    func mqttDidReceivePong(_ mqtt: CocoaMQTT) {}
}
