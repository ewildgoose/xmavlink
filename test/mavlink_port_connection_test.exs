defmodule XMAVLink.PortConnectionTest do
  @moduledoc """
  Port connections: process-owned router connections registered with
  `Router.register_port/3`, injected with `Router.port_inject/3`, receiving
  routed frames as `{:xmavlink_port, port_id, %Frame{}}`.
  """

  use ExUnit.Case

  alias XMAVLink.Frame
  alias XMAVLink.Router

  @secret_key :binary.copy(<<42>>, 32)
  @link_id 9
  @valid_timestamp 10_000_001

  describe "registration lifecycle" do
    setup :start_plain_router

    test "register_port rejects duplicate ids", %{router: router} do
      assert :ok = Router.register_port(router, :overlay)
      assert {:error, :already_registered} = Router.register_port(router, :overlay)
      assert :ok = Router.register_port(router, :second)
    end

    test "unregister_port removes the connection and learned routes", %{router: router} do
      assert :ok = Router.register_port(router, :overlay)
      :ok = Router.port_inject(router, :overlay, heartbeat_raw(1, 1))
      state = :sys.get_state(router)
      assert %XMAVLink.PortConnection{} = state.connections[{:port, :overlay}]
      assert state.routes[{1, 1}] == {:port, :overlay}

      assert :ok = Router.unregister_port(router, :overlay)
      state = :sys.get_state(router)
      refute Map.has_key?(state.connections, {:port, :overlay})
      assert state.routes == %{}
      assert state.port_monitors == %{}

      assert {:error, :not_registered} = Router.unregister_port(router, :overlay)
    end

    test "port owner crash removes the connection and its routes", %{router: router} do
      owner = spawn(fn -> Process.sleep(:infinity) end)
      assert :ok = Router.register_port(router, :overlay, owner)
      :ok = Router.port_inject(router, :overlay, heartbeat_raw(1, 1))
      assert :sys.get_state(router).routes[{1, 1}] == {:port, :overlay}

      Process.exit(owner, :kill)

      wait_until(fn ->
        not Map.has_key?(:sys.get_state(router).connections, {:port, :overlay})
      end)

      state = :sys.get_state(router)
      assert state.routes == %{}
      assert state.port_monitors == %{}
    end

    test "port_inject to an unregistered port is dropped safely", %{router: router} do
      :ok = Router.port_inject(router, :ghost, heartbeat_raw(1, 1))
      # Router still responsive afterwards.
      assert :ok = Router.register_port(router, :ghost)
    end
  end

  describe "ingress (port_inject)" do
    setup :start_router_with_trap

    test "injected broadcast frames reach other connections byte-exact and teach routes",
         %{router: router, trap: trap} do
      :ok = Router.register_port(router, :overlay)
      raw = heartbeat_raw(1, 1)
      :ok = Router.port_inject(router, :overlay, raw)

      assert_receive {:udp, ^trap, _ip, _port, ^raw}, 500
      assert :sys.get_state(router).routes[{1, 1}] == {:port, :overlay}
    end

    test "split horizon: an injected broadcast is not routed back to its own port, but does
          reach local subscribers",
         %{router: router} do
      :ok = Router.register_port(router, :overlay)
      :ok = Router.subscribe(router, message: Common.Message.Heartbeat, as_frame: true)

      :ok = Router.port_inject(router, :overlay, heartbeat_raw(1, 1))

      assert_receive %Frame{source_system: 1, message: %Common.Message.Heartbeat{}}, 500
      refute_receive {:xmavlink_port, :overlay, _}, 200
    end

    test "MAVLink 2 trailing-zero payload truncation is preserved byte-exact",
         %{router: router, trap: trap} do
      :ok = Router.register_port(router, :overlay)

      # SYSTEM_TIME with time_boot_ms == 0: the packed v2 payload ends in
      # zeros and pack_frame truncates it below the full 12 bytes.
      frame = packed_frame(%Common.Message.SystemTime{time_unix_usec: 5, time_boot_ms: 0}, 1, 1)
      raw = frame.mavlink_2_raw
      assert <<0xFD, truncated_payload_length, _::binary>> = raw
      assert truncated_payload_length < 12

      :ok = Router.port_inject(router, :overlay, raw)
      assert_receive {:udp, ^trap, _ip, _port, ^raw}, 500
    end

    test "unknown message ids follow forward_unknown (default :broadcast) byte-exact",
         %{router: router, trap: trap} do
      :ok = Router.register_port(router, :overlay)

      raw =
        Frame.pack_frame(%Frame{
          version: 2,
          sequence_number: 3,
          source_system: 6,
          source_component: 1,
          message_id: 999_999,
          payload: <<1, 2, 3>>,
          crc_extra: 0
        }).mavlink_2_raw

      :ok = Router.port_inject(router, :overlay, raw)
      assert_receive {:udp, ^trap, _ip, _port, ^raw}, 500
    end

    test "signed frames are rejected when the router has no signing configured",
         %{router: router, trap: trap} do
      :ok = Router.register_port(router, :overlay)
      :ok = Router.port_inject(router, :overlay, signed_heartbeat_raw())

      refute_receive {:udp, ^trap, _ip, _port, _}, 200
      # Router alive and the port still works.
      :ok = Router.port_inject(router, :overlay, heartbeat_raw(1, 1))
      assert_receive {:udp, ^trap, _ip, _port, _}, 500
    end
  end

  describe "ingress with signing configured" do
    test "valid signed frames pass through a port byte-exact" do
      {:ok, trap} = :gen_udp.open(0, [:binary, active: true])
      {:ok, trap_port} = :inet.port(trap)

      {:ok, router} =
        Router.start_link(%{
          name: nil,
          system: 245,
          component: 250,
          dialect: Common,
          connection_strings: ["udpout:127.0.0.1:#{trap_port}"],
          signing: [
            secret_key: @secret_key,
            link_id: @link_id,
            # Pin the local timestamp so @valid_timestamp is within the
            # initial-lag acceptance window (mirrors mavlink_router_test).
            timestamp: @valid_timestamp - 1,
            accept_unsigned: true
          ]
        })

      on_exit(fn ->
        :gen_udp.close(trap)
        if Process.alive?(router), do: GenServer.stop(router)
      end)

      wait_for_udpout(router)
      :ok = Router.register_port(router, :overlay)

      raw = signed_heartbeat_raw()
      :ok = Router.port_inject(router, :overlay, raw)

      # Already-signed frames are forwarded with their original signature and
      # bytes; the outbound signing policy only signs unsigned frames.
      assert_receive {:udp, ^trap, _ip, _port, ^raw}, 500
    end
  end

  describe "egress (frames routed to the port owner)" do
    setup :start_plain_router

    test "broadcast frames from other connections arrive as {:xmavlink_port, id, %Frame{}}",
         %{router: router} do
      :ok = Router.register_port(router, :overlay)

      :ok = Router.pack_and_send(router, sample_heartbeat(), 2)

      assert_receive {:xmavlink_port, :overlay, frame = %Frame{}}, 500
      assert %Common.Message.Heartbeat{} = frame.message
      assert is_binary(frame.mavlink_2_raw)
    end

    test "targeted frames follow routes learned through the port", %{router: router} do
      :ok = Router.register_port(router, :overlay)
      # Teach the router that vehicle 1/1 lives behind the port.
      :ok = Router.port_inject(router, :overlay, heartbeat_raw(1, 1))

      :ok =
        Router.pack_and_send(
          router,
          %Common.Message.ParamRequestList{target_system: 1, target_component: 1},
          2
        )

      assert_receive {:xmavlink_port, :overlay, %Frame{message_id: 21, target_system: 1}}, 500
    end

    test "unregistered port no longer receives egress", %{router: router} do
      :ok = Router.register_port(router, :overlay)
      :ok = Router.unregister_port(router, :overlay)

      :ok = Router.pack_and_send(router, sample_heartbeat(), 2)
      refute_receive {:xmavlink_port, _, _}, 200
    end
  end

  # --- setups ---

  defp start_plain_router(_context) do
    {:ok, router} =
      Router.start_link(%{
        name: nil,
        system: 245,
        component: 250,
        dialect: Common,
        connection_strings: []
      })

    on_exit(fn -> if Process.alive?(router), do: GenServer.stop(router) end)
    %{router: router}
  end

  defp start_router_with_trap(_context) do
    {:ok, trap} = :gen_udp.open(0, [:binary, active: true])
    {:ok, trap_port} = :inet.port(trap)

    {:ok, router} =
      Router.start_link(%{
        name: nil,
        system: 245,
        component: 250,
        dialect: Common,
        connection_strings: ["udpout:127.0.0.1:#{trap_port}"]
      })

    on_exit(fn ->
      :gen_udp.close(trap)
      if Process.alive?(router), do: GenServer.stop(router)
    end)

    wait_for_udpout(router)
    %{router: router, trap: trap}
  end

  # --- frame construction ---

  defp sample_heartbeat do
    %Common.Message.Heartbeat{
      type: :mav_type_quadrotor,
      autopilot: :mav_autopilot_ardupilotmega,
      base_mode: MapSet.new(),
      custom_mode: 0,
      system_status: :mav_state_active,
      mavlink_version: 3
    }
  end

  defp heartbeat_raw(source_system, source_component) do
    packed_frame(sample_heartbeat(), source_system, source_component).mavlink_2_raw
  end

  defp signed_heartbeat_raw do
    {:ok, frame} =
      Frame.sign_frame(
        packed_frame(sample_heartbeat(), 1, 1),
        @secret_key,
        @link_id,
        @valid_timestamp
      )

    frame.mavlink_2_raw
  end

  defp packed_frame(message, source_system, source_component) do
    {:ok, message_id, {:ok, crc_extra, _expected_length, target}, payload} =
      XMAVLink.Message.pack(message, 2)

    {target_system, target_component} =
      if target != :broadcast do
        {message.target_system, Map.get(message, :target_component, 0)}
      else
        {0, 0}
      end

    Frame.pack_frame(%Frame{
      version: 2,
      sequence_number: 7,
      source_system: source_system,
      source_component: source_component,
      target_system: target_system,
      target_component: target_component,
      target: target,
      message: message,
      message_id: message_id,
      payload: payload,
      crc_extra: crc_extra
    })
  end

  # --- readiness ---

  defp wait_for_udpout(router, attempts \\ 50) do
    state = :sys.get_state(router)

    has_udpout? =
      Enum.any?(state.connections, fn
        {key, %XMAVLink.UDPOutConnection{}} when is_port(key) -> true
        _ -> false
      end)

    cond do
      has_udpout? ->
        :ok

      attempts == 0 ->
        flunk("udpout connection never opened")

      true ->
        Process.sleep(20)
        wait_for_udpout(router, attempts - 1)
    end
  end

  defp wait_until(fun, attempts \\ 100) do
    cond do
      fun.() ->
        :ok

      attempts == 0 ->
        flunk("condition never became true")

      true ->
        Process.sleep(20)
        wait_until(fun, attempts - 1)
    end
  end
end
