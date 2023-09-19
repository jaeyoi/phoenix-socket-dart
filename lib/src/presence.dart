// ignore_for_file: public_member_api_docs

// TODO: This is a very much a non-tested port of the javascript code!
//       Feel free to test, improve and make a pull request.

import 'dart:async';

import 'channel.dart';
import 'message.dart';

// typedef Presence = Map<String, dynamic>;
typedef PresenceState = Map<String, Presence>;
typedef PresenceDiff = Map<String, PresenceState>;

typedef JoinHandler = void Function(
  String key,
  Presence? current,
  Presence joined,
);
typedef LeaveHandler = void Function(
  String key,
  Presence current,
  Presence left,
);

// No-op methods, default values for callbacks.
void _noopWithThreeArgs(String a, Presence? b, Presence c) {}
void _noopWithNoArg() {}

/// A Phoenix Presence client to interact with a backend
/// Phoenix Channel implementing the Presence module.
/// https://hexdocs.pm/phoenix/presence.html
class PhoenixPresence {
  /// Attaches a Phoenix Presence client to listen to
  /// new presence messages on an existing [channel].
  ///
  /// By default, the 'state' and 'diff' event names
  /// which this client listens to are :
  /// - For state events: 'presence_state'
  /// - For diff events: 'presence_diff'
  ///
  /// This can be customized by passing a custom
  /// name map [eventNames] for those events.
  PhoenixPresence({
    required this.channel,
    this.eventNames = const {
      'state': 'presence_state',
      'diff': 'presence_diff'
    },
  }) {
    // Listens to new messages on the [channel] matching [eventNames]
    // and processes them according to [_onMessage].
    _subscription = channel.messages
        .where((message) => eventNames.containsValue(message.event.value))
        .listen(_onMessage);
  }

  /// A Phoenix Channel which implements
  /// the Phoenix Presence module on the backend.
  final PhoenixChannel channel;

  /// A custom map of event names to listen to on the [channel].
  /// Defaults to the standard Phoenix Presence events names:
  /// ```
  /// {'state': 'presence_state', 'diff': 'presence_diff'}
  /// ```
  final Map<String, String> eventNames;

  late StreamSubscription _subscription;

  /// All presences advertised by the Phoenix backend in real time.
  var state = <String, Presence>{};
  var pendingDiffs = <Map<String, Map<String, Presence>>>[];

  String? _joinRef;

  /// Optional callback to react to changes in the client's local presences when
  /// connecting/reconnecting with the server.
  JoinHandler onJoin = _noopWithThreeArgs;

  /// Optional callback to react to changes in the client's local presences when
  /// disconnecting from the server.
  LeaveHandler onLeave = _noopWithThreeArgs;

  /// Optional callback triggered after
  /// a Presence message/event has been processed.
  Function() onSync = _noopWithNoArg;

  /// Checks whether the presence channel is currently syncing with the server.
  bool get inPendingSyncState =>
      _joinRef == null || _joinRef != channel.joinRef;

  /// Gets the name of the 'state' event to listen to if different
  /// than the default 'presence_state'.
  String get stateEventName {
    if (eventNames.containsKey('state')) return eventNames['state']!;
    return 'presence_state';
  }

  /// Gets the name of the 'diff' event to listen to if different
  /// than the default 'presence_diff'.
  String get diffEventName {
    if (eventNames.containsKey('diff')) return eventNames['diff']!;
    return 'presence_diff';
  }

  /// Returns the array of presences, with selected
  /// metadata formatted according to the [chooser] function.
  /// See Example for better understanding and implementation details.
  List<dynamic> list(
    Map<String, Presence> presences, [
    dynamic Function(String, Presence)? chooser,
  ]) {
    chooser = chooser ?? (k, v) => v;
    return _map(presences, (k, v) => chooser!(k, v));
  }

  /// Stops listening to new messages on [channel].
  void dispose() {
    _subscription.cancel();
  }

  // Process new Presence messages.
  void _onMessage(Message message) {
    final payload = message.payload;
    if (payload != null) {
      // Processing of 'state' events.
      if (message.event.value == stateEventName) {
        _joinRef = channel.joinRef;
        state = _syncState(state, _decodeStateFromPayload(payload));
        for (final diff in pendingDiffs) {
          state = _syncDiff(state, diff);
        }
        pendingDiffs = [];
        onSync();

        // Processing of 'diff' events.
      } else if (message.event.value == diffEventName) {
        final diff = _decodeDiffFromPayload(payload);
        if (inPendingSyncState) {
          pendingDiffs.add(diff);
        } else {
          state = _syncDiff(state, diff);
          onSync();
        }
      }
    }
  }

  /// Generates a "state" map with [Presence] objects
  /// from a serialized message payload.
  PresenceState _decodeStateFromPayload(Map<String, dynamic> payload) {
    return payload.map((key, data) => MapEntry(key, Presence(data)));
  }

  /// Generates a "diff" map with [Presence] objects
  /// from a serialized message payload.
  PresenceDiff _decodeDiffFromPayload(Map<String, dynamic> payload) {
    return payload
        .map((key, state) => MapEntry(key, _decodeStateFromPayload(state)));
    //     if ((presences as Map<String, dynamic>).isEmpty) {
    //       return MapEntry(key, <String, Presence>{});
    //     }
    //     try {
    //       return MapEntry(key, presences.map((presenceKey, presence) {
    //         return MapEntry(
    //             presenceKey, Presence.fromJson(presenceKey, presence));
    //       }));
    //     } catch (e) {
    //       return MapEntry(key, <String, Presence>{});
    //     }
    //   });
  }

  ///  Used to sync the list of presences on the server
  ///  with the client's state. Will call [onJoin] and [onLeave] callbacks
  ///  to react to changes in the client's local presences across
  ///  disconnects and reconnects with the server.
  PresenceState _syncState(
    PresenceState currentState,
    PresenceState newState,
  ) {
    final state = _clone(currentState);
    final joins = <String, Presence>{};
    final leaves = <String, Presence>{};

    _map(state, (key, presence) {
      if (!newState.containsKey(key)) {
        leaves[key] = presence;
      }
    });
    _map(newState, (key, newPresence) {
      if (state.containsKey(key)) {
        final currentPresence = state[key]!;
        final newRefs = newPresence.metas.map((m) => m.phxRef).toSet();
        final curRefs = currentPresence.metas.map((m) => m.phxRef).toSet();

        final joinedMetas = newPresence.metas
            .where((m) => !curRefs.contains(m.phxRef))
            .toList();

        final leftMetas = currentPresence.metas
            .where((m) => !newRefs.contains(m.phxRef))
            .toList();

        if (joinedMetas.isNotEmpty) {
          joins[key] = newPresence;
          joins[key]!.metas = joinedMetas;
        }
        if (leftMetas.isNotEmpty) {
          leaves[key] = Presence.clone(currentPresence);
          leaves[key]!.metas = leftMetas;
        }
      } else {
        joins[key] = newPresence;
      }
    });
    return _syncDiff(state, {'joins': joins, 'leaves': leaves});
  }

  ///  Used to sync a diff of presence join and leave
  ///  events from the server, as they happen. Will call [onJoin]
  ///  and [onLeave] callbacks to react to a user
  ///  joining or leaving from a device.
  PresenceState _syncDiff(
    PresenceState currentState,
    PresenceDiff diff,
  ) {
    final state = _clone(currentState);

    final joins = diff['joins'] ?? {};
    final leaves = diff['leaves'] ?? {};

    _map(joins, (key, newPresence) {
      final currentPresence = state[key];
      state[key] = newPresence;
      if (currentPresence != null) {
        final joinedRefs = state[key]!.metas.map((m) => m.phxRef).toSet();
        final curMetas =
            currentPresence.metas.where((m) => !joinedRefs.contains(m.phxRef));
        state[key]!.metas.insertAll(0, curMetas);
      }
      onJoin(key, currentPresence, newPresence);
    });
    _map(leaves, (key, leftPresence) {
      final currentPresence = state[key];
      if (currentPresence == null) return;
      final refsToRemove = leftPresence.metas.map((m) => m.phxRef).toSet();
      currentPresence.metas = (currentPresence.metas)
          .where((m) => !refsToRemove.contains(m.phxRef))
          .toList();
      onLeave(key, currentPresence, leftPresence);
      if (currentPresence.metas.isEmpty) {
        state.remove(key);
      }
    });
    return state;
  }

  List<dynamic> _map(
    PresenceState presences,
    dynamic Function(String, Presence) mapper,
  ) {
    if (presences.isNotEmpty) {
      return presences.entries
          .map((entry) => mapper(entry.key, entry.value))
          .toList();
    } else {
      return [];
    }
  }

  /// Clones a Map<String, Presence> object. Useful for cloning states.
  PresenceState _clone(PresenceState presences) {
    return {...presences};
  }
}

/// Class that encapsulate all presence events for
/// a specific [key] as a list of metadatas events [metas].
// class Presence {
//   Presence.fromJson(this.key, Map<String, dynamic> events)
//       : data = {...events[key]},
//         metas = List<Map<String, dynamic>>.from(events[key]['metas'])
//             .map((meta) => PhoenixPresenceMeta.fromJson(meta))
//             .toList();

//   /// Identify the presence, typically a userId.
//   final String key;

//   /// The raw data
//   final Map<String, dynamic> data;

//   /// A list of all events metadatas for this Presence [key].
//   List<PhoenixPresenceMeta> metas;

//   Presence clone() {
//     final json = _toJson();
//     final key = json.keys.first;
//     return Presence.fromJson(key, json);
//   }

//   Map<String, dynamic> _toJson() {
//     final json = <String, dynamic>{};
//     json[key] = <String, dynamic>{};
//     json[key]['metas'] = metas.map((meta) => meta._toJson()).toList();
//     return json;
//   }
// }

/// Class that encapsulate the various metadata for a [Presence] event.
/// This class only implements the default metadata field [phxRef], all
/// other custom fields are available in the json map [data] (including
/// the 'phxRef' field). It can be extended with custom field as
/// required, see 'example/flutter_presence_app' for an example on
/// how to implement this in your code.
// class PhoenixPresenceMeta {
//   PhoenixPresenceMeta.fromJson(Map<String, dynamic> meta)
//       : data = {...meta},
//         phxRef = meta['phx_ref'];

//   /// The raw data associated with the meta.
//   final Map<String, dynamic> data;

//   /// The [Presence] event reference on the backend Phoenix server.
//   final String phxRef;

//   /// Clones a [PhoenixPresenceMeta] object
//   PhoenixPresenceMeta clone() {
//     final json = _toJson();
//     return PhoenixPresenceMeta.fromJson(json);
//   }

//   Map<String, dynamic> _toJson() => data;
// }

class Presence {
  Presence(this.data)
      : metas = (data['metas'] as List).map((m) => PresenceMeta(m)).toList();
  Presence.clone(Presence original) : this(original.data);

  Map<String, dynamic> data;
  List<PresenceMeta> metas;
}

class PresenceMeta {
  PresenceMeta(Map meta) : phxRef = meta['phx_ref'];

  final String phxRef;
}
