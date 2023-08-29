#coded by 0xbit
import os, sys
from tabulate import tabulate
import re
from pystyle import *

dark = Col.dark_gray
light = Colors.StaticMIX((Col.cyan, Col.purple, Col.gray))
acc = Colors.StaticMIX((Col.cyan, Col.purple, Col.blue, Col.gray))
purple = Colors.StaticMIX((Col.purple, Col.blue))
bpurple = Colors.StaticMIX((Col.purple, Col.cyan))

green_checkmark = "\033[32m✔\033[0m"
red_x = "\033[31m✘\033[0m"  

def check_alert_status():
    os.system('python3 com.py -v alert_detail > .data.txt')
    input_filename = '.data.txt'
    output_filename = '.data.txt'

    with open(input_filename, 'r') as input_file:
        data = input_file.read()

    modified_data = re.sub(r':| {4,}', ' ', data)

    with open(output_filename, 'w') as output_file:
        output_file.write(modified_data)

    with open('.data.txt', 'r') as f:
        lines = f.readlines()

    data = []
    for line in lines:
        splitter = line.split()
        if len(splitter) == 2:
            _name, _stat = splitter
            _stat = green_checkmark if _stat == 'False' else red_x
            data.append([_name, _stat])

    print(tabulate(data, headers=['Name', ''], tablefmt='fancy_grid'))

def check_status():
    os.system('python3 com.py -v status > .data_status.txt')
    input_filename = '.data_status.txt'
    output_filename = '.data_status.txt'

    with open(input_filename, 'r') as input_file:
        data = input_file.read()

    modified_data = re.sub(r':| {7,}', ' ', data)

    modified_data = modified_data.replace('no value', 'none')

    with open(output_filename, 'w') as output_file:
        output_file.write(data)

    with open('.data_status.txt', 'r') as f:
        lines = f.readlines()

    data = []
    for line in lines:
        splitter = line.split()
        if len(splitter) == 2:
            _name, _stat = splitter
            _stat = '' if _stat == '' else _stat
            data.append([_name, _stat])

    print(tabulate(data, headers=['Name', 'Status'], tablefmt='fancy_grid'))

def check_ob():
    os.system('python3 com.py -v obstruction_detail > .data_ob.txt')
    input_filename = '.data_ob.txt'
    output_filename = '.data_ob.txt'

    with open(input_filename, 'r') as input_file:
        data = input_file.read()

    modified_data = re.sub(r':', ' ', data)

    modified_data = modified_data.replace('no value', 'none')

    with open(output_filename, 'w') as output_file:
        output_file.write(data)

    with open('.data_ob.txt', 'r') as f:
        lines = f.readlines()

    data = []
    for line in lines:
        splitter = line.split()
        if len(splitter) == 2:
            _name, _stat = splitter
            data.append([_name, _stat])

    print(tabulate(data, headers=['Name', 'Status'], tablefmt='fancy_grid'))

def check_location():
    os.system('python3 com.py -v location > .data_loc.txt')
    input_filename = '.data_loc.txt'
    output_filename = '.data_loc.txt'

    with open(input_filename, 'r') as input_file:
        lines = input_file.readlines()

    formatted_data = ""
    for line in lines:
        if ':' in line:
            key, value = line.strip().split(':')
            formatted_data += f"{key.strip()} {value.strip()}\n"

    with open(output_filename, 'w') as output_file:
        output_file.write(formatted_data)

    with open(output_filename, 'r') as f:
        lines = f.readlines()

    data = []
    for line in lines:
        _name, _value = line.strip().split()
        data.append([_name, _value])

    print(tabulate(data, tablefmt='fancy_grid', floatfmt=".15f", headers=['Name','Value']))    
    
    with open('.data_loc.txt', 'r') as file:
        data = file.readlines()

    lat = None
    long = None
    
    for line in data:
        parts = line.strip().split()
        if len(parts) == 2:
            if parts[0] == 'latitude':
                lat = float(parts[1])
            elif parts[0] == 'longitude':
                long = float(parts[1])

    
    google_maps_link: str = (f"https://www.google.com/maps?q={lat},{long}&ll={lat},{long}&z=15")
    print(tabulate([['GOOGLE MAP',google_maps_link]], tablefmt='fancy_grid'))

def starlink_stow():
    command: str = r'./dedsec -plaintext -d {\"dish_stow\":{}} 192.168.100.1:9200 SpaceX.API.Device.Device/Handle > /dev/null 2>&1'
    os.system(command)
    print(tabulate([['DISH STATUS','STOW']], tablefmt='fancy_grid'))
    input(' PRESS ENTER TO EXIT')
    return main()

def starlink_untow():
    command: str = r'./dedsec -plaintext -d {\"dish_stow\":{\"unstow\":true}} 192.168.100.1:9200 SpaceX.API.Device.Device/Handle > /dev/null 2>&1'
    os.system(command)
    print(tabulate([['DISH STATUS','UNTOW']], tablefmt='fancy_grid'))
    input(' PRESS ENTER TO EXIT')
    return main()

def starlink_reboot():
    command: str = r'./dedsec -plaintext -d {\"reboot\":{}} 192.168.100.1:9200 SpaceX.API.Device.Device/Handle > /dev/null 2>&1'
    os.system(command)
    print(tabulate([['DISH STATUS','REBOOTED']], tablefmt='fancy_grid'))
    input(' PRESS ENTER TO EXIT')
    return main()

def banner():
    def p(text):
    # sleep(0.05)
        return print(stage(text))


    def stage(text: str, symbol: str = '...', col1=light, col2=None) -> str:
        if col2 is None:
            col2 = light if symbol == '...' else purple
        if symbol in {'...', '!!!'}:
            return f"""     {Col.Symbol(symbol, col1, dark)} {col2}{text}{Col.reset}"""
        else:
            return f""" {Col.Symbol(symbol, col1, dark)} {col2}{text}{Col.reset}"""


    text = r"""

            .       .                   .       .      .     .      .

                .    .         .    .            .     ______

          .           .             .               ////////

                    .    .   ________   .  .      /////////     .    .

               .            |.____.  /\        ./////////    .

        .                 .//      \/  |\     /////////

          .       .    .//          \ |  \ /////////       .     .   .

                       ||.    .    .| |  ///////// .     .

        .    .         ||           | |//`,/////                .

                .       \\        ./ //  /  \/   .

    .                    \\.___./ //\` '   ,_\     .     .

            .           .     \ //////\ , /   \                 .    .

                         .    ///////// \|  '  |    .

        .        .          ///////// .   \ _ /          .

                          /////////                              .

                   .   ./////////     .     .

            .           --------   .                  ..             .


    """

    text1 = '''                       STARLINK MONITORING TOOL
                            CODED BY: 0XBIT
                
                    
            [01] STATUS                                [04] STOW
            [02] ALERT STATUS                          [05] UNSTOW
            [03] LOCATION                              [06] REBOOT
            
            [00] EXIT'''

    print(Colorate.Diagonal(Colors.DynamicMIX((purple, dark)), (text)))
    print(((purple)), (text1))

def main():
    os.system('clear')
    banner()
    select: int = input('\n\t    STARLINK: ')
    if select in ['01', '1']:
        check_status()
        input(' PRESS ENTER TO EXIT')
        return main()
    elif select in ['02','2']:
        check_alert_status()
        input(' PRESS ENTER TO EXIT')
        return main()
    elif select in ['03', '3']:
        check_location()
        input(' PRESS ENTER TO EXIT')
        return main()
    elif select in ['04', '4']:
        starlink_stow()
        input(' PRESS ENTER TO EXIT')
        return main()
    elif select in ['05', '5']:
        starlink_untow()
        input(' PRESS ENTER TO EXIT')
        return main()
    elif select in ['06', '6']:
        starlink_reboot()
        input(' PRESS ENTER TO EXIT')
        return main()
    elif select in ['00', '0']:
        sys.exit('        BYE BYE! ')

if __name__ == '__main__':
    main()
