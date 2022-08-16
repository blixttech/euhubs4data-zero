from utils.zerounit import *
from utils.zerounitthread import zerounitthread

if __name__ == "__main__":
    print('Starting Blixt Zero ADC logger, please provide comma separated IP numbers to all sensors that you wish to log')
    parser = argparse.ArgumentParser(description='ADC sample collection client for Blixt Zero')
    parser.add_argument('address', metavar='ADDRESS', type=str,
                        help='IP number as string')
    #parser.add_argument('-output', '--output', metavar='OUTPUT', default=None, type=str, help='Output CSV file')

    args = parser.parse_args()
    config = vars(args)
    givenAddressesArg = config['address']
    print('Given address string: ' + givenAddressesArg)
    urls = givenAddressesArg.split(',')
    print('Comma separated urls ' + str(urls))
    print('---')

    zerounits = {}
    
    try:
        for url in urls:
            zero_info = zerounitinfo()
            hostname = urllib.parse.urlsplit('//' + url).hostname
            #print('url ' + hostname)
            
            zero_info.remote_addr = hostname
            zerounits[url] = zerounitthread(zero_info)

        while True:
            time.sleep(1)
    except KeyboardInterrupt as e:
        print("Stopping...")

    for zu in zerounits.values():
        zu.info.stop_thread = True
        
    time.sleep(3)
    print('All stopped')
