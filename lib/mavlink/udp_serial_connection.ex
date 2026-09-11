defmodule XMAVLink.UDPSerialConnection do
  @moduledoc """
  A `udpserial:<host>:<port>[:<local_port>]` connection: UDP transport carrying a MAVLink
  **byte stream** rather than framed datagrams.

  Serial-to-ethernet adapters chop the autopilot's serial stream into UDP
  datagrams at arbitrary boundaries: one datagram may contain dozens of
  concatenated frames, or a fragment of one. `udpout:` parses per datagram
  (correct for real MAVLink UDP peers) and silently loses everything beyond
  the first frame boundary; this connection instead buffers across datagrams
  like the serial/TCP transports — extracting complete frames, keeping the
  remainder, skipping garbage, and bounding the buffer.

  Outbound behaves exactly like `udpout:` (send to the configured address;
  the adapter relays to serial). Like any UDP peer of such an adapter, the
  proxy must transmit once before the adapter knows where to send the
  stream (`MavProxy.prime/0` in the downstream proxy).

  ## Mis-sync recovery

  A saturated serial link drops bytes mid-frame, so the parser can read
  past a truncated payload and swallow the frames that follow. On a
  checksum failure this connection therefore resyncs from one byte past
  the false start-of-frame magic, recovering frames embedded in the bytes
  it had consumed, rather than discarding them.

  Limit: when a mis-sync lands on an *unknown* message id the checksum
  cannot be verified at all (crc_extra comes from the dialect), so that
  case is indistinguishable from a genuinely unknown message and is
  relayed per `forward_unknown`; the stream re-syncs on subsequent data.
  """

  @behaviour XMAVLink.Transport

  require Logger
  import XMAVLink.Utils, only: [format_address: 1]

  alias XMAVLink.Connection.Inbound
  alias XMAVLink.Connection.Outbound
  alias XMAVLink.ConnectionWorker
  alias XMAVLink.Frame

  @smallest_mavlink_message 8
  @max_stream_buffer_size 4_096

  defstruct address: nil,
            port: nil,
            local_port: 0,
            socket: nil,
            worker: nil,
            signing: nil,
            buffer: <<>>

  @type t :: %XMAVLink.UDPSerialConnection{
          address: XMAVLink.Types.net_address(),
          port: XMAVLink.Types.net_port(),
          local_port: non_neg_integer,
          socket: port,
          worker: pid | nil,
          signing: XMAVLink.Signing.t() | nil,
          buffer: binary
        }

  def handle_info({:udp, socket, source_addr, source_port, raw}, connection, dialect) do
    buffer = connection.buffer <> raw

    case Frame.binary_to_frame_and_tail(buffer) do
      :not_a_frame ->
        if byte_size(buffer) > 0 do
          Logger.debug("UDPSerialConnection.handle_info: no frame in #{byte_size(buffer)} bytes")
        end

        {:error, :not_a_frame, socket, struct(connection, buffer: <<>>)}

      {nil, rest} when byte_size(rest) > @max_stream_buffer_size ->
        Logger.debug(
          "UDPSerialConnection.handle_info: dropping overlong stream buffer (#{byte_size(rest)} bytes)"
        )

        {:error, :stream_buffer_overflow, socket, struct(connection, buffer: <<>>)}

      {nil, rest} ->
        {:error, :incomplete_frame, socket, struct(connection, buffer: rest)}

      {frame, rest} ->
        # Re-feed the extracted frame's exact bytes through the standard
        # datagram pipeline: validation, signing, and unknown-message policy
        # all behave identically to every other connection type.
        raw_frame = frame.mavlink_2_raw || frame.mavlink_1_raw

        result =
          Inbound.datagram(
            raw_frame,
            struct(connection, buffer: rest),
            socket,
            dialect,
            "UDPSerialConnection.handle_info",
            "from #{format_address(source_addr)}:#{source_port}"
          )

        case result do
          {:error, :checksum_invalid, connection_key, errored_connection} ->
            # We mis-framed: either a stray byte matched the start-of-frame
            # magic, or the serial link lost bytes mid-frame so we read past
            # its end and swallowed what followed. Real frames commonly
            # begin INSIDE the bytes we just consumed, so resync from one
            # byte past the false magic instead of discarding them.
            resync =
              binary_part(raw_frame, 1, byte_size(raw_frame) - 1) <> errored_connection.buffer

            rescan(socket, source_addr, source_port)

            {:error, :checksum_invalid, connection_key,
             struct(errored_connection, buffer: clamp_buffer(resync))}

          other ->
            # More complete frames may be waiting in the remainder.
            if byte_size(rest) >= @smallest_mavlink_message do
              rescan(socket, source_addr, source_port)
            end

            other
        end
    end
  end

  defp rescan(socket, source_addr, source_port),
    do: send(self(), {:udp, socket, source_addr, source_port, <<>>})

  defp clamp_buffer(buffer) when byte_size(buffer) <= @max_stream_buffer_size, do: buffer

  defp clamp_buffer(buffer),
    do: binary_part(buffer, byte_size(buffer) - @max_stream_buffer_size, @max_stream_buffer_size)

  def open(["udpserial", address, port], controlling_process),
    do: open(["udpserial", address, port, 0], controlling_process)

  # A fixed local port (the fourth token) keeps the same address across a
  # restart, so an adapter that only sends to the address it last heard
  # from needs no new heartbeat to find us again. 0 = any free port.
  def open(["udpserial", address, port, local_port], controlling_process) do
    case :gen_udp.open(local_port, [:binary, active: true] ++ family_options(address)) do
      {:ok, socket} ->
        {:ok, bound} = :inet.port(socket)

        :ok =
          Logger.info(
            "Opened udpserial:#{format_address(address)}:#{port} from local port #{bound}"
          )

        :ok = :gen_udp.controlling_process(socket, controlling_process)

        {:ok, socket,
         struct(
           XMAVLink.UDPSerialConnection,
           socket: socket,
           address: address,
           port: port,
           local_port: local_port,
           worker: controlling_process
         )}

      other ->
        {:error, other}
    end
  end

  defp family_options(address) when is_tuple(address) and tuple_size(address) == 8, do: [:inet6]
  defp family_options(_address), do: []

  def close(%XMAVLink.UDPSerialConnection{socket: socket}) do
    :gen_udp.close(socket)
  end

  def forward(connection = %XMAVLink.UDPSerialConnection{worker: worker}, frame)
      when is_pid(worker) do
    ConnectionWorker.forward(worker, connection, frame)
  end

  def forward(%XMAVLink.UDPSerialConnection{socket: socket, address: address, port: port}, frame) do
    :gen_udp.send(socket, address, port, Outbound.packet!(frame))
  end
end
