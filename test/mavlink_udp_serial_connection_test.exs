defmodule XMAVLink.UDPSerialConnectionTest do
  @moduledoc """
  `udpserial:` — byte-stream MAVLink over UDP (serial-to-ethernet
  adapters): frames split across datagrams are reassembled, concatenated
  frames in one datagram are all delivered, garbage is skipped, and
  outbound forwarding reaches the adapter address.
  """

  use ExUnit.Case

  alias XMAVLink.Frame
  alias XMAVLink.Router

  setup do
    # The "adapter": a plain UDP socket the router's udpserial dials.
    {:ok, adapter} = :gen_udp.open(0, [:binary, active: true])
    {:ok, adapter_port} = :inet.port(adapter)

    {:ok, router} =
      Router.start_link(%{
        name: nil,
        system: 245,
        component: 250,
        dialect: Common,
        connection_strings: ["udpserial:127.0.0.1:#{adapter_port}"]
      })

    :ok = Router.subscribe(router, message: Common.Message.Heartbeat, as_frame: true)

    # Wait for the connection to register, then learn its local port so the
    # adapter can stream to it.
    socket = wait_for_udpserial_socket(router)
    {:ok, proxy_port} = :inet.port(socket)

    on_exit(fn ->
      :gen_udp.close(adapter)

      try do
        GenServer.stop(router)
      catch
        :exit, _ -> :ok
      end
    end)

    %{adapter: adapter, router: router, proxy_port: proxy_port}
  end

  defp stream(adapter, proxy_port, chunk),
    do: :ok = :gen_udp.send(adapter, {127, 0, 0, 1}, proxy_port, chunk)

  test "a frame split across datagrams is reassembled",
       %{adapter: adapter, proxy_port: proxy_port} do
    raw = heartbeat_raw(1, 1)
    <<part1::binary-9, part2::binary>> = raw

    stream(adapter, proxy_port, part1)
    refute_receive %Frame{}, 100
    stream(adapter, proxy_port, part2)

    assert_receive %Frame{source_system: 1, message: %Common.Message.Heartbeat{}}, 1_000
  end

  test "many concatenated frames in one datagram are all delivered",
       %{adapter: adapter, proxy_port: proxy_port} do
    burst = for sys <- 1..10, into: <<>>, do: heartbeat_raw(sys, 1)
    stream(adapter, proxy_port, burst)

    for sys <- 1..10 do
      assert_receive %Frame{source_system: ^sys}, 1_000
    end
  end

  test "an adapter-style chop (big burst + fragments) loses nothing",
       %{adapter: adapter, proxy_port: proxy_port} do
    # 6 frames, chopped at arbitrary boundaries like a serial adapter does.
    all = for sys <- 1..6, into: <<>>, do: heartbeat_raw(sys, 1)
    <<c1::binary-3, c2::binary-40, c3::binary-1, rest::binary>> = all

    for chunk <- [c1, c2, c3, rest], do: stream(adapter, proxy_port, chunk)

    for sys <- 1..6 do
      assert_receive %Frame{source_system: ^sys}, 1_000
    end
  end

  test "a frame following a TRUNCATED frame is still recovered (resync)",
       %{adapter: adapter, proxy_port: proxy_port} do
    # Exactly the bench failure: the serial link loses bytes mid-frame, so
    # the parser reads past the short payload and swallows the next frame's
    # header. Resyncing one byte past the false magic must recover it.
    truncated = binary_part(heartbeat_raw(9, 1), 0, 12)
    good = heartbeat_raw(5, 1)

    stream(adapter, proxy_port, truncated <> good)

    assert_receive %Frame{source_system: 5}, 1_000
  end

  test "the stream keeps flowing after a mis-sync that cannot be checksummed",
       %{adapter: adapter, proxy_port: proxy_port} do
    # A stray magic byte can mis-frame into an UNKNOWN message id, whose
    # checksum cannot be verified (crc_extra comes from the dialect), so
    # that mis-sync is undetectable by design. What must hold is that the
    # stream re-syncs on subsequent data rather than wedging.
    stream(adapter, proxy_port, <<0xFD, 20, 0, 0>> <> heartbeat_raw(6, 1))
    stream(adapter, proxy_port, heartbeat_raw(7, 1) <> heartbeat_raw(8, 1))

    assert_receive %Frame{source_system: 8}, 1_000
  end

  test "garbage between frames is skipped", %{adapter: adapter, proxy_port: proxy_port} do
    stream(adapter, proxy_port, "failsafe garbage" <> heartbeat_raw(3, 1) <> <<0, 0, 0>>)
    assert_receive %Frame{source_system: 3}, 1_000

    # And the trailing zeros don't wedge the stream.
    stream(adapter, proxy_port, heartbeat_raw(4, 1))
    assert_receive %Frame{source_system: 4}, 1_000
  end

  test "outbound frames reach the adapter address",
       %{adapter: adapter, router: router} do
    :ok =
      Router.pack_and_send(
        router,
        %Common.Message.Heartbeat{
          type: :mav_type_gcs,
          autopilot: :mav_autopilot_invalid,
          base_mode: MapSet.new(),
          custom_mode: 0,
          system_status: :mav_state_active,
          mavlink_version: 3
        },
        2
      )

    assert_receive {:udp, ^adapter, _ip, _port, data}, 1_000
    assert <<0xFD, _::binary>> = data
  end

  test "a fourth token fixes the local port, so an adapter finds us again after a restart" do
    {:ok, probe} = :gen_udp.open(0, [:binary])
    {:ok, local_port} = :inet.port(probe)
    :gen_udp.close(probe)

    assert %{
             transport: XMAVLink.UDPSerialConnection,
             tokens: ["udpserial", {127, 0, 0, 1}, 10_002, ^local_port]
           } = XMAVLink.ConnectionSpec.parse("udpserial:127.0.0.1:10002:#{local_port}")

    assert_raise ArgumentError, ~r/invalid local port/, fn ->
      XMAVLink.ConnectionSpec.parse("udpserial:127.0.0.1:10002:x")
    end

    {:ok, router} =
      Router.start_link(%{
        name: nil,
        system: 246,
        component: 250,
        dialect: Common,
        connection_strings: ["udpserial:127.0.0.1:10002:#{local_port}"]
      })

    socket = wait_for_udpserial_socket(router)
    assert {:ok, ^local_port} = :inet.port(socket)
    GenServer.stop(router)
  end

  # --- helpers ---

  defp heartbeat_raw(source_system, source_component) do
    message = %Common.Message.Heartbeat{
      type: :mav_type_quadrotor,
      autopilot: :mav_autopilot_ardupilotmega,
      base_mode: MapSet.new(),
      custom_mode: 0,
      system_status: :mav_state_active,
      mavlink_version: 3
    }

    {:ok, message_id, {:ok, crc_extra, _len, _target}, payload} =
      XMAVLink.Message.pack(message, 2)

    Frame.pack_frame(%Frame{
      version: 2,
      sequence_number: 7,
      source_system: source_system,
      source_component: source_component,
      message_id: message_id,
      payload: payload,
      crc_extra: crc_extra
    }).mavlink_2_raw
  end

  defp wait_for_udpserial_socket(router, attempts \\ 50) do
    state = :sys.get_state(router)

    found =
      Enum.find(state.connections, fn
        {key, %XMAVLink.UDPSerialConnection{}} when is_port(key) -> true
        _ -> false
      end)

    cond do
      found != nil ->
        {socket, _} = found
        socket

      attempts == 0 ->
        flunk("udpserial connection never opened")

      true ->
        Process.sleep(20)
        wait_for_udpserial_socket(router, attempts - 1)
    end
  end
end
