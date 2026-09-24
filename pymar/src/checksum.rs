use crate::format::ChecksumType;

const XXHASH_PRIME64_1: u64 = 0x9E3779B185EBCA87;
const XXHASH_PRIME64_2: u64 = 0xC2B2AE3D27D4EB4F;
const XXHASH_PRIME64_3: u64 = 0x165667B19E3779F9;
#[allow(dead_code)]
const XXHASH_PRIME64_4: u64 = 0x85EBCA77C2B2AE63;
#[allow(dead_code)]
const XXHASH_PRIME64_5: u64 = 0x27D4EB2F165667C5;

#[inline]
fn rotl64(x: u64, r: u32) -> u64 {
    x.rotate_left(r)
}

#[inline]
fn avalanche64(mut v: u64) -> u64 {
    v ^= v >> 33;
    v = v.wrapping_mul(XXHASH_PRIME64_2);
    v ^= v >> 29;
    v = v.wrapping_mul(XXHASH_PRIME64_3);
    v ^= v >> 32;
    v
}

#[derive(Clone)]
pub struct XXHash3_64 {
    seed: u64,
    acc0: u64,
    acc1: u64,
    acc2: u64,
    acc3: u64,
    total_len: u64,
    buffer: [u8; 32],
    buffer_size: usize,
}

impl XXHash3_64 {
    pub fn new(seed: u64) -> Self {
        let mut s = Self {
            seed,
            acc0: 0,
            acc1: 0,
            acc2: 0,
            acc3: 0,
            total_len: 0,
            buffer: [0u8; 32],
            buffer_size: 0,
        };
        s.reset();
        s
    }

    pub fn reset(&mut self) {
        self.acc0 = self.seed.wrapping_add(XXHASH_PRIME64_1).wrapping_add(XXHASH_PRIME64_2);
        self.acc1 = self.seed.wrapping_add(XXHASH_PRIME64_2);
        self.acc2 = self.seed;
        self.acc3 = self.seed.wrapping_sub(XXHASH_PRIME64_1);
        self.total_len = 0;
        self.buffer_size = 0;
    }

    #[inline]
    fn process_stripe(&mut self, p: &[u8]) {
        let lane0 = u64::from_le_bytes(p[0..8].try_into().unwrap());
        let lane1 = u64::from_le_bytes(p[8..16].try_into().unwrap());
        let lane2 = u64::from_le_bytes(p[16..24].try_into().unwrap());
        let lane3 = u64::from_le_bytes(p[24..32].try_into().unwrap());

        self.acc0 = self.acc0.wrapping_add(lane0.wrapping_mul(XXHASH_PRIME64_2));
        self.acc0 = rotl64(self.acc0, 31).wrapping_mul(XXHASH_PRIME64_1);

        self.acc1 = self.acc1.wrapping_add(lane1.wrapping_mul(XXHASH_PRIME64_2));
        self.acc1 = rotl64(self.acc1, 31).wrapping_mul(XXHASH_PRIME64_1);

        self.acc2 = self.acc2.wrapping_add(lane2.wrapping_mul(XXHASH_PRIME64_2));
        self.acc2 = rotl64(self.acc2, 31).wrapping_mul(XXHASH_PRIME64_1);

        self.acc3 = self.acc3.wrapping_add(lane3.wrapping_mul(XXHASH_PRIME64_2));
        self.acc3 = rotl64(self.acc3, 31).wrapping_mul(XXHASH_PRIME64_1);
    }

    pub fn update(&mut self, data: &[u8]) {
        if data.is_empty() {
            return;
        }
        self.total_len += data.len() as u64;
        let mut p = data;

        if self.buffer_size > 0 {
            let to_fill = 32 - self.buffer_size;
            if p.len() < to_fill {
                self.buffer[self.buffer_size..self.buffer_size + p.len()].copy_from_slice(p);
                self.buffer_size += p.len();
                return;
            }
            self.buffer[self.buffer_size..32].copy_from_slice(&p[..to_fill]);
            let buf = self.buffer;
            self.process_stripe(&buf);
            p = &p[to_fill..];
            self.buffer_size = 0;
        }

        while p.len() >= 32 {
            self.process_stripe(&p[..32]);
            p = &p[32..];
        }

        if !p.is_empty() {
            self.buffer[..p.len()].copy_from_slice(p);
            self.buffer_size = p.len();
        }
    }

    pub fn finalize(&self) -> u64 {
        let mut acc0 = self.acc0;
        let mut acc1 = self.acc1;
        let mut acc2 = self.acc2;
        let mut acc3 = self.acc3;

        if self.buffer_size > 0 {
            let mut buf = [0u8; 32];
            buf[..self.buffer_size].copy_from_slice(&self.buffer[..self.buffer_size]);
            let lane0 = u64::from_le_bytes(buf[0..8].try_into().unwrap());
            let lane1 = u64::from_le_bytes(buf[8..16].try_into().unwrap());
            let lane2 = u64::from_le_bytes(buf[16..24].try_into().unwrap());
            let lane3 = u64::from_le_bytes(buf[24..32].try_into().unwrap());

            acc0 = acc0.wrapping_add(lane0.wrapping_mul(XXHASH_PRIME64_2));
            acc0 = rotl64(acc0, 31).wrapping_mul(XXHASH_PRIME64_1);

            acc1 = acc1.wrapping_add(lane1.wrapping_mul(XXHASH_PRIME64_2));
            acc1 = rotl64(acc1, 31).wrapping_mul(XXHASH_PRIME64_1);

            acc2 = acc2.wrapping_add(lane2.wrapping_mul(XXHASH_PRIME64_2));
            acc2 = rotl64(acc2, 31).wrapping_mul(XXHASH_PRIME64_1);

            acc3 = acc3.wrapping_add(lane3.wrapping_mul(XXHASH_PRIME64_2));
            acc3 = rotl64(acc3, 31).wrapping_mul(XXHASH_PRIME64_1);
        }

        let mut result = self.total_len
            .wrapping_add(rotl64(acc0, 1))
            .wrapping_add(rotl64(acc1, 7))
            .wrapping_add(rotl64(acc2, 12))
            .wrapping_add(rotl64(acc3, 18));

        result = avalanche64(result);
        result
    }

    pub fn finalize_32(&self) -> u32 {
        (self.finalize() & 0xFFFFFFFF) as u32
    }
}

pub fn xxhash3_256_from_u64(digest: u64) -> [u8; 32] {
    let mut out = [0u8; 32];
    for i in 0..4 {
        out[i * 8..(i + 1) * 8].copy_from_slice(&digest.to_le_bytes());
    }
    out
}

pub fn xxhash3_256(data: &[u8]) -> [u8; 32] {
    let mut hasher = XXHash3_64::new(0);
    hasher.update(data);
    xxhash3_256_from_u64(hasher.finalize())
}

pub const CRC32C_TABLE: [u32; 256] = [
    0x00000000, 0xF26B8303, 0xE13B70F7, 0x1350F3F4, 0xC79A971F, 0x35F1141C, 0x26A1E7E8, 0xD4CA64EB, 0x8AD958CF,
    0x78B2DBCC, 0x6BE22838, 0x9989AB3B, 0x4D43CFD0, 0xBF284CD3, 0xAC78BF27, 0x5E133C24, 0x105EC76F, 0xE235446C,
    0xF165B798, 0x030E349B, 0xD7C45070, 0x25AFD373, 0x36FF2087, 0xC494A384, 0x9A879FA0, 0x68EC1CA3, 0x7BBCEF57,
    0x89D76C54, 0x5D1D08BF, 0xAF768BBC, 0xBC267848, 0x4E4DFB4B, 0x20BD8EDE, 0xD2D60DDD, 0xC186FE29, 0x33ED7D2A,
    0xE72719C1, 0x154C9AC2, 0x061C6936, 0xF477EA35, 0xAA64D611, 0x580F5512, 0x4B5FA6E6, 0xB93425E5, 0x6DFE410E,
    0x9F95C20D, 0x8CC531F9, 0x7EAEB2FA, 0x30E349B1, 0xC288CAB2, 0xD1D83946, 0x23B3BA45, 0xF779DEAE, 0x05125DAD,
    0x1642AE59, 0xE4292D5A, 0xBA3A117E, 0x4851927D, 0x5B016189, 0xA96AE28A, 0x7DA08661, 0x8FCB0562, 0x9C9BF696,
    0x6EF07595, 0x417B1DBC, 0xB3109EBF, 0xA0406D4B, 0x522BEE48, 0x86E18AA3, 0x748A09A0, 0x67DAFA54, 0x95B17957,
    0xCBA24573, 0x39C9C670, 0x2A993584, 0xD8F2B687, 0x0C38D26C, 0xFE53516F, 0xED03A29B, 0x1F682198, 0x5125DAD3,
    0xA34E59D0, 0xB01EAA24, 0x42752927, 0x96BF4DCC, 0x64D4CECF, 0x77843D3B, 0x85EFBE38, 0xDBFC821C, 0x2997011F,
    0x3AC7F2EB, 0xC8AC71E8, 0x1C661503, 0xEE0D9600, 0xFD5D65F4, 0x0F36E6F7, 0x61C69362, 0x93AD1061, 0x80FDE395,
    0x72966096, 0xA65C047D, 0x5437877E, 0x4767748A, 0xB50CF789, 0xEB1FCBAD, 0x197448AE, 0x0A24BB5A, 0xF84F3859,
    0x2C855CB2, 0xDEEEDFB1, 0xCDBE2C45, 0x3FD5AF46, 0x7198540D, 0x83F3D70E, 0x90A324FA, 0x62C8A7F9, 0xB602C312,
    0x44694011, 0x5739B3E5, 0xA55230E6, 0xFB410CC2, 0x092A8FC1, 0x1A7A7C35, 0xE811FF36, 0x3CDB9BDD, 0xCEB018DE,
    0xDDE0EB2A, 0x2F8B6829, 0x82F63B78, 0x70BDB87B, 0x63ED4B8F, 0x9186C88C, 0x454CAC67, 0xB7272F64, 0xA477DC90,
    0x561C5F93, 0x080F63B7, 0xFA64E0B4, 0xE9341340, 0x1B5F9043, 0xCF95F4A8, 0x3DFE77AB, 0x2EAE845F, 0xDCC5075C,
    0x92884917, 0x60E3CA14, 0x73B339E0, 0x81D8BAE3, 0x5512DE08, 0xA7795D0B, 0xB429AEFF, 0x46422DFC, 0x185111D8,
    0xEA3A92DB, 0xF96A612F, 0x0B01E22C, 0xDFCB86C7, 0x2DA005C4, 0x3EF0F630, 0xCC9B7533, 0x84D68DDF, 0x76BD0EDC,
    0x65EDFD28, 0x97867E2B, 0x434C1AC0, 0xB12799C3, 0xA2776A37, 0x501CE934, 0x0E0FD510, 0xFC645613, 0xEF34A5E7,
    0x1D5F26E4, 0xC995420F, 0x3BFEC10C, 0x28AE32F8, 0xDAC5B1FB, 0x94884AB0, 0x66E3C9B3, 0x75B33A47, 0x87D8B944,
    0x5312DDAF, 0xA1795EAC, 0xB229AD58, 0x40422E5B, 0x1E51127F, 0xEC3A917C, 0xFF6A6288, 0x0D01E18B, 0xD9CB8560,
    0x2BA00663, 0x38F0F597, 0xCA9B7694, 0xE010C1B9, 0x127B42BA, 0x012BB14E, 0xF340324D, 0x278A56A6, 0xD5E1D5A5,
    0xC6B12651, 0x34DAA552, 0x6AC99976, 0x98A21A75, 0x8BF2E981, 0x79996A82, 0xAD530E69, 0x5F388D6A, 0x4C687E9E,
    0xBE03FD9D, 0xF04E06D6, 0x0225859D, 0x11757669, 0xE31EF56A, 0x37D49181, 0xC5BF1282, 0xD6EFE176, 0x24846275,
    0x7A975E51, 0x88FCDD52, 0x9BAC2EA6, 0x69C7ADA5, 0xBD0DC94E, 0x4F664A4D, 0x5C36B9B9, 0xAE5D3ABA, 0xC0ADD8FF,
    0x32C65BFC, 0x2196A808, 0xD3FD2B0B, 0x07374FE0, 0xF55CCCE3, 0xE60C3F17, 0x1467BC14, 0x4A748030, 0xB81F0333,
    0xAB4FF0C7, 0x592473C4, 0x8DEE172F, 0x7F85942C, 0x6CD567D8, 0x9EBEE4DB, 0xD0F31F90, 0x22989C93, 0x31C86F67,
    0xC3A3EC64, 0x1769888F, 0xE5020B8C, 0xF652F878, 0x04397B7B, 0x5A2A475F, 0xA841C45C, 0xBB1137A8, 0x497AB4AB,
    0x9DB0D040, 0x6FDB5343, 0x7C8BA0B7, 0x8EE023B4,
];

pub fn crc32c(data: &[u8]) -> u32 {
    let mut crc = 0xFFFFFFFFu32;
    for &b in data {
        crc = CRC32C_TABLE[((crc ^ (b as u32)) & 0xFF) as usize] ^ (crc >> 8);
    }
    crc ^ 0xFFFFFFFF
}

pub fn blake3(data: &[u8]) -> [u8; 32] {
    *blake3::hash(data).as_bytes()
}

pub fn blake3_32(data: &[u8]) -> u32 {
    let hash = blake3(data);
    u32::from_le_bytes(hash[0..4].try_into().unwrap())
}

pub fn xxhash32(data: &[u8], seed: u32) -> u32 {
    use std::hash::Hasher;
    let mut hasher = twox_hash::XxHash32::with_seed(seed);
    hasher.write(data);
    hasher.finish() as u32
}

pub fn compute_fast_checksum(data: &[u8], checksum_type: ChecksumType) -> u32 {
    match checksum_type {
        ChecksumType::None => 0,
        ChecksumType::Blake3 => blake3_32(data),
        ChecksumType::XXHash32 => xxhash32(data, 0),
        ChecksumType::XXHash3 => {
            let mut hasher = XXHash3_64::new(0);
            hasher.update(data);
            hasher.finalize_32()
        }
        ChecksumType::Crc32c => crc32c(data),
    }
}

pub fn verify_fast_checksum(data: &[u8], checksum_type: ChecksumType, expected: u32) -> bool {
    if checksum_type == ChecksumType::None || expected == 0 {
        return true;
    }
    compute_fast_checksum(data, checksum_type) == expected
}

pub fn hash_to_hex(data: &[u8]) -> String {
    let mut s = String::with_capacity(data.len() * 2);
    for b in data {
        s.push_str(&format!("{:02x}", b));
    }
    s
}

#[derive(Clone)]
pub struct Md5Hasher {
    state: [u32; 4],
    count: u64,
    buffer: [u8; 64],
}

impl Default for Md5Hasher {
    fn default() -> Self {
        Self::new()
    }
}

impl Md5Hasher {
    pub fn new() -> Self {
        Self {
            state: [0x67452301, 0xefcdab89, 0x98badcfe, 0x10325476],
            count: 0,
            buffer: [0u8; 64],
        }
    }

    fn transform(&mut self, block: &[u8; 64]) {
        let mut a = self.state[0];
        let mut b = self.state[1];
        let mut c = self.state[2];
        let mut d = self.state[3];

        let mut m = [0u32; 16];
        for i in 0..16 {
            m[i] = u32::from_le_bytes(block[i * 4..(i + 1) * 4].try_into().unwrap());
        }

        macro_rules! ff {
            ($a:ident, $b:ident, $c:ident, $d:ident, $k:expr, $s:expr, $t:expr) => {
                $a = $a.wrapping_add(($b & $c) | (!$b & $d)).wrapping_add(m[$k]).wrapping_add($t);
                $a = $a.rotate_left($s).wrapping_add($b);
            };
        }
        macro_rules! gg {
            ($a:ident, $b:ident, $c:ident, $d:ident, $k:expr, $s:expr, $t:expr) => {
                $a = $a.wrapping_add(($b & $d) | ($c & !$d)).wrapping_add(m[$k]).wrapping_add($t);
                $a = $a.rotate_left($s).wrapping_add($b);
            };
        }
        macro_rules! hh {
            ($a:ident, $b:ident, $c:ident, $d:ident, $k:expr, $s:expr, $t:expr) => {
                $a = $a.wrapping_add($b ^ $c ^ $d).wrapping_add(m[$k]).wrapping_add($t);
                $a = $a.rotate_left($s).wrapping_add($b);
            };
        }
        macro_rules! ii {
            ($a:ident, $b:ident, $c:ident, $d:ident, $k:expr, $s:expr, $t:expr) => {
                $a = $a.wrapping_add($c ^ ($b | !$d)).wrapping_add(m[$k]).wrapping_add($t);
                $a = $a.rotate_left($s).wrapping_add($b);
            };
        }

        // Round 1
        ff!(a, b, c, d,  0,  7, 0xd76aa478);
        ff!(d, a, b, c,  1, 12, 0xe8c7b756);
        ff!(c, d, a, b,  2, 17, 0x242070db);
        ff!(b, c, d, a,  3, 22, 0xc1bdceee);
        ff!(a, b, c, d,  4,  7, 0xf57c0faf);
        ff!(d, a, b, c,  5, 12, 0x4787c62a);
        ff!(c, d, a, b,  6, 17, 0xa8304613);
        ff!(b, c, d, a,  7, 22, 0xfd469501);
        ff!(a, b, c, d,  8,  7, 0x698098d8);
        ff!(d, a, b, c,  9, 12, 0x8b44f7af);
        ff!(c, d, a, b, 10, 17, 0xffff5bb1);
        ff!(b, c, d, a, 11, 22, 0x895cd7be);
        ff!(a, b, c, d, 12,  7, 0x6b901122);
        ff!(d, a, b, c, 13, 12, 0xfd987193);
        ff!(c, d, a, b, 14, 17, 0xa679438e);
        ff!(b, c, d, a, 15, 22, 0x49b40821);

        // Round 2
        gg!(a, b, c, d,  1,  5, 0xf61e2562);
        gg!(d, a, b, c,  6,  9, 0xc040b340);
        gg!(c, d, a, b, 11, 14, 0x265e5a51);
        gg!(b, c, d, a,  0, 20, 0xe9b6c7aa);
        gg!(a, b, c, d,  5,  5, 0xd62f105d);
        gg!(d, a, b, c, 10,  9, 0x02441453);
        gg!(c, d, a, b, 15, 14, 0xd8a1e681);
        gg!(b, c, d, a,  4, 20, 0xe7d3fbc8);
        gg!(a, b, c, d,  9,  5, 0x21e1cde6);
        gg!(d, a, b, c, 14,  9, 0xc33707d6);
        gg!(c, d, a, b,  3, 14, 0xf4d50d87);
        gg!(b, c, d, a,  8, 20, 0x455a14ed);
        gg!(a, b, c, d, 13,  5, 0xa9e3e905);
        gg!(d, a, b, c,  2,  9, 0xfcefa3f8);
        gg!(c, d, a, b,  7, 14, 0x676f02d9);
        gg!(b, c, d, a, 12, 20, 0x8d2a4c8a);

        // Round 3
        hh!(a, b, c, d,  5,  4, 0xfffa3942);
        hh!(d, a, b, c,  8, 11, 0x8771f681);
        hh!(c, d, a, b, 11, 16, 0x6d9d6122);
        hh!(b, c, d, a, 14, 23, 0xfde5380c);
        hh!(a, b, c, d,  1,  4, 0xa4beea44);
        hh!(d, a, b, c,  4, 11, 0x4bdecfa9);
        hh!(c, d, a, b,  7, 16, 0xf6bb4b60);
        hh!(b, c, d, a, 10, 23, 0xbebfbc70);
        hh!(a, b, c, d, 13,  4, 0x289b7ec6);
        hh!(d, a, b, c,  0, 11, 0xeaa127fa);
        hh!(c, d, a, b,  3, 16, 0xd4ef3085);
        hh!(b, c, d, a,  6, 23, 0x04881d05);
        hh!(a, b, c, d,  9,  4, 0xd9d4d039);
        hh!(d, a, b, c, 12, 11, 0xe6db99e5);
        hh!(c, d, a, b, 15, 16, 0x1fa27cf8);
        hh!(b, c, d, a,  2, 23, 0xc4ac5665);

        // Round 4
        ii!(a, b, c, d,  0,  6, 0xf4292244);
        ii!(d, a, b, c,  7, 10, 0x432aff97);
        ii!(c, d, a, b, 14, 15, 0xab9423a7);
        ii!(b, c, d, a,  5, 21, 0xfc93a039);
        ii!(a, b, c, d, 12,  6, 0x655b59c3);
        ii!(d, a, b, c,  3, 10, 0x8f0ccc92);
        ii!(c, d, a, b, 10, 15, 0xffeff47d);
        ii!(b, c, d, a,  1, 21, 0x85845dd1);
        ii!(a, b, c, d,  8,  6, 0x6fa87e4f);
        ii!(d, a, b, c, 15, 10, 0xfe2ce6e0);
        ii!(c, d, a, b,  6, 15, 0xa3014314);
        ii!(b, c, d, a, 13, 21, 0x4e0811a1);
        ii!(a, b, c, d,  4,  6, 0xf7537e82);
        ii!(d, a, b, c, 11, 10, 0xbd3af235);
        ii!(c, d, a, b,  2, 15, 0x2ad7d2bb);
        ii!(b, c, d, a,  9, 21, 0xeb86d391);

        self.state[0] = self.state[0].wrapping_add(a);
        self.state[1] = self.state[1].wrapping_add(b);
        self.state[2] = self.state[2].wrapping_add(c);
        self.state[3] = self.state[3].wrapping_add(d);
    }

    pub fn update(&mut self, data: &[u8]) {
        let idx = (self.count as usize) % 64;
        self.count += data.len() as u64;

        let mut offset = 0;
        if idx > 0 {
            let space = 64 - idx;
            if data.len() >= space {
                self.buffer[idx..64].copy_from_slice(&data[..space]);
                let block = self.buffer;
                self.transform(&block);
                offset = space;
            } else {
                self.buffer[idx..idx + data.len()].copy_from_slice(data);
                return;
            }
        }

        while offset + 64 <= data.len() {
            let block: [u8; 64] = data[offset..offset + 64].try_into().unwrap();
            self.transform(&block);
            offset += 64;
        }

        if offset < data.len() {
            let rem = data.len() - offset;
            self.buffer[..rem].copy_from_slice(&data[offset..]);
        }
    }

    pub fn finalize(&self) -> [u8; 16] {
        let mut clone = self.clone();
        let bit_count = clone.count * 8;
        let idx = (clone.count as usize) % 64;
        let pad_len = if idx < 56 { 56 - idx } else { 120 - idx };

        let mut padding = [0u8; 128];
        padding[0] = 0x80;
        padding[pad_len..pad_len + 8].copy_from_slice(&bit_count.to_le_bytes());

        clone.update(&padding[..pad_len + 8]);

        let mut out = [0u8; 16];
        for (i, &s) in clone.state.iter().enumerate() {
            out[i * 4..(i + 1) * 4].copy_from_slice(&s.to_le_bytes());
        }
        out
    }
}
