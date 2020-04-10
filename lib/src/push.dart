import 'dart:async';

import 'package:logging/logging.dart';
import 'package:quiver/collection.dart';
import 'package:equatable/equatable.dart';

import 'channel.dart';
import 'events.dart';
import 'exception.dart';
import 'message.dart';

// ignore: avoid_catches_without_on_clauses

/// Encapsulates the response to a [Push].
class PushResponse implements Equatable {
  /// Status provided by the backend.
  ///
  /// Value is usually either 'ok' or 'error'.
  final String status;

  /// Arbitrary JSON content provided by the backend.
  final dynamic response;

  /// Builds a PushResponse from a status and response.
  PushResponse({
    this.status,
    this.response,
  });

  /// Builds a PushResponse from a Map payload.
  ///
  /// Standard is such that the payload should be something like
  /// `{status: "ok", response: {foo: "bar"}}`
  factory PushResponse.fromPayload(Map<String, dynamic> data) {
    return PushResponse(
      status: data['status'] as String,
      response: data['response'],
    );
  }

  /// Whether the response as a 'ok' status.
  bool get isOk => status == 'ok';

  /// Whether the response as a 'error' status.
  bool get isError => status == 'error';

  @override
  List<Object> get props => [status, response];

  @override
  bool get stringify => true;
}

typedef PayloadGetter = Map<String, dynamic> Function();

class Push {
  final Logger _logger;
  final PhoenixChannelEvent event;
  final PayloadGetter payload;
  final PhoenixChannel _channel;
  final ListMultimap<String, void Function(PushResponse)> _receivers =
      ListMultimap();

  Duration timeout;
  PushResponse _received;
  bool _sent = false;
  bool _boundCompleter = false;
  Timer _timeoutTimer;
  String _ref;

  String get ref => _ref ??= _channel.socket.nextRef;

  Completer<PushResponse> _responseCompleter;
  Future<PushResponse> get future {
    _responseCompleter ??= Completer<PushResponse>();
    return _responseCompleter.future;
  }

  Push(
    PhoenixChannel channel, {
    this.event,
    this.payload,
    this.timeout,
  })  : _channel = channel,
        _logger = Logger('phoenix_socket.push.${channel.loggerName}');

  bool get sent => _sent;
  PhoenixChannelEvent __replyEvent;

  PhoenixChannelEvent get _replyEvent =>
      __replyEvent ??= PhoenixChannelEvent.replyFor(ref);

  bool hasReceived(String status) => _received?.status == status;

  void onReply(
    String status,
    void Function(PushResponse) callback,
  ) {
    _receivers[status].add(callback);
  }

  void cancelTimeout() {
    _timeoutTimer?.cancel();
    _timeoutTimer = null;
  }

  void reset() {
    cancelTimeout();
    _channel.removeWaiters(_replyEvent);
    _received = null;
    _ref = null;
    __replyEvent = null;
    _sent = false;
    _responseCompleter = null;
  }

  void clearWaiters() {
    _receivers.clear();
    _responseCompleter = null;
  }

  void trigger(PushResponse response) {
    _received = response;

    if (_responseCompleter != null) {
      if (_responseCompleter.isCompleted) {
        _logger.warning('Push being completed more than once');
        _logger.warning(
          () => '  event: $_replyEvent, status: ${response.status}',
        );
        _logger.finer(
          () => '  response: ${response.response}',
        );
        return;
      } else {
        _logger.finer(
          () =>
              'Completing for $_replyEvent with response ${response.response}',
        );
        _responseCompleter.complete(response);
      }
    }
    _logger.finer(() {
      if (_receivers[response.status].isNotEmpty) {
        return 'Triggering ${_receivers[response.status].length} callbacks';
      }
      return 'Not triggering any callbacks';
    });
    for (final cb in _receivers[response.status]) {
      cb(response);
    }
    _receivers.clear();
    _channel.removeWaiters(_replyEvent);
  }

  void _receiveResponse(dynamic response) {
    if (response is Message) {
      cancelTimeout();
      if (response.event == _replyEvent) {
        trigger(PushResponse.fromPayload(response.payload));
      }
    } else if (response is PhoenixException) {
      cancelTimeout();
      if (_responseCompleter is Completer) {
        _responseCompleter.completeError(response);
      }
    }
  }

  void startTimeout() {
    if (!_boundCompleter) {
      _channel.onPushReply(_replyEvent)
        ..then(_receiveResponse)
        ..catchError(_receiveResponse);
      _boundCompleter = true;
    }

    _timeoutTimer ??= Timer(timeout, () {
      _timeoutTimer = null;
      _logger.warning('Push $_ref timed out');
      _channel.trigger(Message(
        event: _replyEvent,
        payload: {
          'status': 'timeout',
          'response': {},
        },
      ));
    });
  }

  Future<void> send() async {
    if (hasReceived('timeout')) {
      _logger.warning('Trying to send push $_ref after timeout');
      return;
    }
    _sent = true;
    _boundCompleter = false;

    startTimeout();
    try {
      await _channel.socket.sendMessage(Message(
        event: event,
        topic: _channel.topic,
        payload: payload(),
        ref: ref,
        joinRef: _channel.joinRef,
      ));
    } catch (err) {
      _receiveResponse(err);
    }
  }

  Future<void> resend(Duration newTimeout) async {
    timeout = newTimeout ?? timeout;
    reset();
    await send();
  }
}
