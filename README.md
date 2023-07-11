# LanternDB 🏮

Relational and Vector Database

To build and install:
```bash
git clone --recursive git@github.com:Ngalstyan4/pgembedding.git
cd pgembedding
mkdir build
cd build
cmake ..
make install
make test
```

To install on M1 macs without building usearch with `march=native`, replace `cmake ..` from the abvoe with
```bash
cmake -DUSEARCH_NO_MARCH_NATIVE=ON ..
```
