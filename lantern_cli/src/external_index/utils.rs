pub fn bytes_to_integer_le<T>(bytes: &[u8]) -> T
where
    T: From<u8> + std::ops::Shl<usize, Output = T> + std::ops::BitOr<Output = T> + Default,
{
    let mut result: T = Default::default();

    for &byte in bytes.iter().rev() {
        result = (result << 8) | T::from(byte);
    }

    result
}
