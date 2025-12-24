import can
import time
from typing import Set

# --- Configuration ---
BUS_INTERFACE = 'can0'
SCAN_DURATION_SECONDS = 120  # 5 Minutes

# Comprehensive list of IDs from the data definition images
DEFINED_IDS = {
    0x1801A0A1: "SBCU1_Basic_Inf",
    0x0C02A0A1: "SBCU1_State_Inf1",
    0x1803A0A1: "SBCU1_State_Inf2",
    0x1804A0A1: "SBCU1_SOP_Inf1",
    0x1805A0A1: "SBCU1_SOP_Inf2",
    0x1806A0A1: "SBCU1_Extrem_Inf1",
    0x1807A0A1: "SBCU1_Extrem_Inf2",
    0x1808A0A1: "SBCU1_Debug_Inf",
    0x18F000A1: "SBCU1_Cell_VoltInf1",
    0x18F800A1: "SBCU1_TempInf1",
    0x18FC00A1: "SBCU1_Cell_BalanceInf1",
    0x180AA0A1: "SBCU1_Statis_AccumCap",
    0x180BA0A1: "SBCU1_Statis_AccumEng",
    0x0C0101A0: "MBCU_Cmd_Info1",
    0x180201A0: "MBCU_State_Inf1",
    0x180202A0: "MBCU_State_Inf2",
    0x6D1:      "DIAG_HSCA",
    0x6E1:      "DIAG_HSCB",
    0x6F1:      "DIAG_HSCC",
    0x180101B1: "Tester_Cmd",
    0x1802F4F3: "Tester_Time_Cmd",
    0x180203A0: "MBCU_Curr_TIME",
    0x0C0102A0: "MBCU_Cmd_Info2",
    0x180204A0: "MBCU_SBCU_ID_Calib",
    0x1809A0A1: "SBCU1_State_Inf3",
    0x1810A0A1: "SBCU1_State_Inf4",
    0x1FF:      "Tester_Slience_Msg_ExtCAN",
    0x1811A0A1: "SBCU1_State_Inf5",
    0x1812A0A1: "SBCU1_State_Inf6",
    0x6:        "ExCAN_Calib_Cmd"
}

def scan_bus():
    print(f"Connecting to {BUS_INTERFACE}...")
    found_ids: Set[int] = set()
    
    try:
        # No filters set to ensure we capture everything on the bus
        bus = can.interface.Bus(channel=BUS_INTERFACE, interface='socketcan')
        
        start_time = time.time()
        print(f"Starting 5-minute scan at {time.strftime('%H:%M:%S')}...")
        print("Listening for all message IDs...")

        while (time.time() - start_time) < SCAN_DURATION_SECONDS:
            # Check remaining time
            elapsed = time.time() - start_time
            remaining = SCAN_DURATION_SECONDS - elapsed
            
            message = bus.recv(1.0) # Wait up to 1s for a message
            
            if message is not None:
                found_ids.add(message.arbitration_id)
            
            print(f"Time Remaining: {int(remaining)}s | Unique IDs Found: {len(found_ids)}", end='\r')

        print("\n\nScan complete.")

    except KeyboardInterrupt:
        print("\nScan interrupted by user.")
    finally:
        if 'bus' in locals():
            bus.shutdown()

    # --- Generate Report ---
    print("\n" + "="*40)
    print("MESSAGES NOT FOUND IN BUS")
    print("="*40)
    
    missing_count = 0
    print(f"{'ID (Hex)':<12} | {'Message Name'}")
    print("-" * 40)
    
    for def_id in sorted(DEFINED_IDS.keys()):
        if def_id not in found_ids:
            print(f"{hex(def_id):<12} | {DEFINED_IDS[def_id]}")
            missing_count += 1
            
    if missing_count == 0:
        print("All defined IDs were successfully detected on the bus.")
    else:
        print("-" * 40)
        print(f"Total Missing: {missing_count} out of {len(DEFINED_IDS)}")

if __name__ == '__main__':
    scan_bus()