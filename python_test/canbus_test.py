import can
import struct
import time
from typing import List

# --- Configuration ---
BUS_INTERFACE = 'can0' 

# CAN IDs defined in the protocol document
MBCU_CMD_INFO1_ID    = 0x0C0101A0  # ID for life signal/heartbeat
SBCU1_BASIC_INF_ID   = 0x1801A0A1  
MBCU_CURR_TIME_ID    = 0x180203A0  
SBCU1_STATE_INF1_ID  = 0x0C02A0A1  
SBCU1_ACCUM_CAP_ID   = 0x180AA0A1  
SBCU1_ACCUM_ENG_ID   = 0x180BA0A1  

# ======================================================================
# DECODING FUNCTIONS
# ======================================================================

def decode_basic_inf(data: bytes):
    v_pack = struct.unpack('>H', data[0:2])[0] * 0.1
    v_link = struct.unpack('>H', data[2:4])[0] * 0.1
    raw_curr = struct.unpack('>h', data[4:6])[0]
    curr = (raw_curr * 0.1) - 3200
    soc = struct.unpack('>H', data[6:8])[0] * 0.1
    return {"pack_v": v_pack, "link_v": v_link, "curr": curr, "soc": soc}

def decode_state_inf1(data: bytes):
    work_modes = {0: "Invalid", 1: "Work", 2: "Maintenance", 3: "Fault Stop"}
    mode = work_modes.get(data[0] & 0x0F, "Unknown")
    hv_step = (data[1] >> 4) & 0x0F
    relay_map = {0: "Open", 1: "Closed", 2: "Fault", 3: "Fault"}
    pos_relay = relay_map.get(data[3] & 0x03)
    neg_relay = relay_map.get((data[3] >> 2) & 0x03)
    pre_relay = relay_map.get((data[3] >> 4) & 0x03)
    life_signal = (data[4] >> 4) & 0x0F #
    fault_code = (data[5] << 8) | data[6] #
    fault_lvl = (data[7] >> 2) & 0x03 #
    return {
        "mode": mode, "hv_step": hv_step, "pos_relay": pos_relay, 
        "neg_relay": neg_relay, "pre_relay": pre_relay, 
        "life_signal": life_signal, "fault_code": fault_code, "fault_lvl": fault_lvl
    }

def decode_bess_time(data: bytes):
    year = data[0] + 1900
    month, day = data[1], data[2]
    hour, minute, second = data[3], data[4], data[5]
    return f"{year}-{month:02d}-{day:02d} {hour:02d}:{minute:02d}:{second:02d}"\
    
def send_hv_on_command(bus, heartbeat_value, sbcu_index=0):
    """
    Sends the HV Power ON command for a specific SBCU.
    sbcu_index: 0 to 15 (representing which SBCU to turn on)
    """
    # Initialize an 8-byte payload
    payload = [0] * 8
    
    # 1. Set the HV Power ON bitmask (Bytes 0-1)
    # We use a 16-bit integer and pack it into the first two bytes
    # To turn ON a specific SBCU, we set its corresponding bit to 1
    hv_mask = 1 << sbcu_index
    payload[0] = (hv_mask >> 8) & 0xFF  # High byte (Byte 0)
    payload[1] = hv_mask & 0xFF         # Low byte (Byte 1)
    
    # 2. Maintain the Life Signal (Byte 7, bits 4-7)
    payload[7] = (heartbeat_value << 4) & 0xF0
    
    # Construct and send the message
    hv_on_msg = can.Message(
        arbitration_id=0x0C0101A0, # MBCU_Cmd_Info1_ID
        data=payload,
        is_extended_id=True
    )
    
    print(f"[DEBUG] Constructing HV ON command: ID=0x{hv_on_msg.arbitration_id:X}, Payload={' '.join(f'{b:02X}' for b in payload)}, SBCU Index={sbcu_index}")
    
    try:
        bus.send(hv_on_msg)
        print(f"[DEBUG] ✓ HV ON Command sent for SBCU {sbcu_index}")
    except can.CanError as e:
        print(f"[DEBUG] ✗ Failed to send HV ON command: {e}")

def clear_fault_command(bus, sbcu_index=0):
    """
    Sends the Common Fault Clearing command (0x0C0102A0) for a specific SBCU.
    sbcu_index: 0 to 15
    """
    # Initialize an 8-byte payload
    payload = [0] * 8
    
    # Bytes 0-1: Common fault clearing command (16 bits for 16 SBCUs)
    # 0: not cleared; 1: cleared
    fault_clear_mask = 1 << sbcu_index
    payload[0] = (fault_clear_mask >> 8) & 0xFF  # High byte
    payload[1] = fault_clear_mask & 0xFF         # Low byte
    
    msg = can.Message(
        arbitration_id=0x0C0102A0, # MBCU_Cmd_Info2
        data=payload,
        is_extended_id=True
    )
    
    try:
        bus.send(msg)
        print(f"Sent Common Fault Clear for SBCU {sbcu_index}")
    except can.CanError as e:
        print(f"Failed to send Fault Clear: {e}")

def send_sbcu_status_reset(bus, heartbeat_value):
    """
    Triggers the SBCU status reset via MBCU_Cmd_Info1 (0x0C0101A0).
    Byte 4, bits 6-7: 1 = Reset
    """
    payload = [0] * 8
    
    # Byte 4, Bits 6-7: SBCU status reset
    # 0: Not reset; 1: Reset
    payload[4] = (0x01 << 6) 
    
    # Maintain your cyclic Life Signal in Byte 7
    payload[7] = (heartbeat_value << 4) & 0xF0
    
    msg = can.Message(
        arbitration_id=0x0C0101A0,
        data=payload,
        is_extended_id=True
    )
    
    bus.send(msg)
    print("Sent SBCU Status Reset command")

# ======================================================================
# MONITORING AND CONTROL ENGINE
# ======================================================================


def monitor_bess():
    print(f"Connecting to {BUS_INTERFACE}...")
    try:
        bus = can.interface.Bus(channel=BUS_INTERFACE, interface='socketcan')
        
        bus.set_filters([
            {'can_id': SBCU1_BASIC_INF_ID, 'can_mask': 0x1FFFFFFF, 'extended': True},
            {'can_id': SBCU1_STATE_INF1_ID, 'can_mask': 0x1FFFFFFF, 'extended': True},
            {'can_id': MBCU_CURR_TIME_ID, 'can_mask': 0x1FFFFFFF, 'extended': True}
        ])

        bess_stats = {"time": "Waiting...", "soc": 0.0, "mode": "N/A", "fault": 0, "cap": {}, "eng": {}}
        
        # --- State Variables ---
        last_heartbeat_time = time.time()
        heartbeat_value = 0
        hv_power_on_requested = False  # Track the HV state globally
        loop_start_time = time.time()

        #send initial reset command
        send_sbcu_status_reset(bus, heartbeat_value)

        # send clear fault command at start
        clear_fault_command(bus, sbcu_index=0)  # Assuming SBCU index 0 for clearing faults
        
        while True:
            current_time = time.time()

            # 1. TRIGGER HV ON STATE (After 10 seconds)
            if not hv_power_on_requested and (current_time - loop_start_time) >= 10:
                print(f"\n[SYSTEM] 10s elapsed. Changing state to HV Power ON...")
                hv_power_on_requested = True

            # 2. SEND PERIODIC MESSAGE (Every 50ms - 500ms as per protocol)
            # Note: Protocol 0x0C0101A0 recommends a 50ms cycle time
            if current_time - last_heartbeat_time >= 0.5:
                heartbeat_value = (heartbeat_value + 1) % 16 # Cycle 0-15 as per scale
                
                payload = [0] * 8
                
                # Apply HV Power ON bitmask if requested
                if hv_power_on_requested:
                    # Setting bit 0 for SBCU 1
                    payload[1] = 0x01 
                else:
                    payload[0] = 0x00
                    payload[1] = 0x00
                
                # Apply Life Signal (Heartbeat) to Byte 7 bits 4-7
                payload[7] = (heartbeat_value << 4) & 0xF0
                
                msg = can.Message(
                    arbitration_id=MBCU_CMD_INFO1_ID,
                    data=payload,
                    is_extended_id=True
                )
                bus.send(msg)
                last_heartbeat_time = current_time

            # 3. RECEIVE MESSAGES
            msg = bus.recv(0.01) 
            if msg:
                if msg.arbitration_id == SBCU1_BASIC_INF_ID:
                    bess_stats.update(decode_basic_inf(msg.data))
                elif msg.arbitration_id == MBCU_CURR_TIME_ID:
                    bess_stats["time"] = decode_bess_time(msg.data)
                elif msg.arbitration_id == SBCU1_STATE_INF1_ID:
                    bess_stats.update(decode_state_inf1(msg.data))
                
                # Dashboard Output
                print(f"[{bess_stats['time']}] SOC: {bess_stats['soc']}% | Mode: {bess_stats['mode']} | "
                      f"BMS Life: {bess_stats.get('life_signal', 0)} | "
                      f"Fault Lvl: {bess_stats.get('fault_lvl', 0)} Fault code: {hex(bess_stats.get('fault_code', 0))}  | "
                      f"HV Status: {'ON' if hv_power_on_requested else 'OFF'} | "
                      f"HB: {heartbeat_value} ", end='\r')

    except KeyboardInterrupt:
        print("\nMonitoring stopped.")
    finally:
        if 'bus' in locals(): bus.shutdown()


if __name__ == '__main__':
    monitor_bess()