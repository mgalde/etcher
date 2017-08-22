const usbboot = require('./lib/shared/sdk/usbboot')
const fs = require('fs')

usbboot.scan({
  files: {
    'bootcode.bin': fs.readFileSync('../usbboot/msd/bootcode.bin'),
    'start.elf': fs.readFileSync('../usbboot/msd/start.elf')
  }
}).then((devices) => {
  console.log(devices)
}).then(() => {
  console.log('Done')
}).catch((error) => {
  console.log('There was an error')
  console.error(error)
})
