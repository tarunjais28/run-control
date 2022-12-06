pub static mut DEF_SOC_ADDR: String = String::new();

pub fn get_socket_address(addr: String) {
    unsafe {
        DEF_SOC_ADDR = addr;
    }
}
