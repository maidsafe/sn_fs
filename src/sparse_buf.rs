use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq)]
pub struct SparseBuf {
    /// logical size
    pub size: u64,
    data: BTreeMap<u64,u8>,
}

impl SparseBuf {

    pub fn new() -> Self {
        SparseBuf {
            size: 0,
            data: BTreeMap::new(),
        }
    }

    pub fn with_capacity(size: u64) -> Self {
        SparseBuf {
            size,
            data: BTreeMap::new(),
        }
    }

    pub fn resize(&mut self, offset: u64) {
        self.size = offset;
        self.data.split_off(&offset);
    }

    pub fn write(&mut self, offset: u64, buf: &[u8]) -> bool {
        if offset >= self.size {
            return false;
        }

        for (i, byte) in buf.iter().enumerate() {
            self.data.insert(offset+(i as u64), *byte);
        }
        true
    }

    pub fn read(&self, offset: u64, len: u64) -> Vec<u8> {
        let mut buf: Vec<u8> = Vec::with_capacity(len as usize);

        if offset < self.size {
            let readlen = if self.size < offset + len { self.size - offset } else { len };
            
            for i in 0..readlen {
                match self.data.get(&(offset+i)) {
                    Some(byte) => buf.push(*byte),
                    None => buf.push(0),
                }
            }
        }
        buf
    }

    pub fn readall(&self) -> Vec<u8> {
        self.read(0, self.size)
    }
}


#[cfg(test)]
mod test {

    use super::*;

    #[test]
    fn test_resize() {
        let mut b: SparseBuf = SparseBuf::new();

        assert_eq!(b.size, 0);

        b.resize(2000);
        assert_eq!(b.size, 2000);
    }    

    #[test]
    fn test_full_write() {
        let buf = b"werwaeaksdjf askdfjasdkweiriweruiweasdkskdkak ewarkwe kwerjkew kasda";
        let mut b: SparseBuf = SparseBuf::with_capacity(buf.len() as u64);
        b.write(0, buf);

        let bytes_read = b.readall();
        assert_eq!(bytes_read, buf.to_vec() );
    }    

    #[test]
    fn test_partial_write() {
        let mut b: SparseBuf = SparseBuf::with_capacity(1000);

        b.write(25, b"23423432423423");
        b.write(224, b"abcdef");

        let bytes_read = b.read(223, 8);
        assert_eq!(&bytes_read, &b"\0abcdef\0".to_vec() );
    }    

}