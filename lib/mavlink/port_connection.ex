defmodule XMAVLink.PortConnection do
  @moduledoc """
  A router connection whose transport is an owning Elixir process (a "port")
  rather than a socket.

  Ports are the integration surface for overlay transports, bridges, and
  protocol translators that need to move raw MAVLink wire bytes in and out of
  a router as a first-class connection: routes are learned per port, split
  horizon applies (a frame is never routed back to the port it arrived from),
  unknown-message frames follow the router's `forward_unknown` policy, and
  egress is byte-exact (an unsigned frame's original bytes — including
  MAVLink 2 payload truncation and existing signatures — are delivered
  verbatim; outbound signing applies only when the router has a signing
  policy, exactly as for socket connections).

  ## Usage

      :ok = XMAVLink.Router.register_port(router, :overlay)

      # Inject raw wire bytes received from elsewhere:
      :ok = XMAVLink.Router.port_inject(router, :overlay, raw_binary)

      # Frames the router routes to this port arrive in the owner's mailbox:
      receive do
        {:xmavlink_port, :overlay, frame = %XMAVLink.Frame{}} ->
          transmit(frame.mavlink_2_raw || frame.mavlink_1_raw)
      end

  The registering process is monitored: if it exits, the port and any routes
  learned through it are removed.
  """

  alias XMAVLink.Connection.Inbound
  alias XMAVLink.Frame
  alias XMAVLink.PortConnection

  defstruct port_id: nil, pid: nil, signing: nil

  @type t :: %PortConnection{
          port_id: term,
          pid: pid,
          signing: XMAVLink.Signing.t() | nil
        }

  @doc false
  # Parse/validate injected raw bytes on the identical code path used for UDP
  # datagrams, so route learning, unknown-message policy, and signing
  # validation behave the same as for any other connection.
  def handle_inject(raw, connection = %PortConnection{port_id: port_id}, dialect) do
    Inbound.datagram(raw, connection, {:port, port_id}, dialect, "port #{inspect(port_id)}")
  end

  @doc false
  def forward(%PortConnection{port_id: port_id, pid: pid}, frame = %Frame{}) do
    send(pid, {:xmavlink_port, port_id, frame})
  end
end
