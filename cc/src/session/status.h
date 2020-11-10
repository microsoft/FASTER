// Copyright (c) University of Utah. All rights reserved.
// Licensed under the MIT license.

#pragma once

namespace session {

/// This class defines the different states a session can be in.
enum class Status : uint8_t {
  /// The session was created recently. The server and client sides are
  /// yet to agree on the view number they will operate under. Requests
  /// cannot be processed when the session is in this state.
  INITIALIZING,

  /// The session has been initialized. Requests can be served/processed.
  OPEN,

  /// The view on the server changed. The client should update it's ownership
  /// mappings and reissue operations.
  VIEW_CHANGED,

  /// The server has asked the client to stop sending requests for a small
  /// interval of time.
  BACK_OFF,

  /// The session was closed abruptly. This session can be reused in the
  /// future if the client end supplies the correct session-uid.
  DISCONNECTED,

  /// The session was closed gracefully. Requests will no longer be
  /// received/processed on this session. This session can never be restarted
  /// again.
  CLOSED,
};

} // end of namespace session
