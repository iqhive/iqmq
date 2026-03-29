#include <stdio.h>
#define XXH_STATIC_LINKING_ONLY
#define XXH_IMPLEMENTATION
#define XXH_VECTOR XXH_SCALAR
#include "xxhash.h"
#include "keys.h"
int main(void) {
  uint8_t buf[KAT_BUF]; kat_fill(buf, KAT_BUF);
  printf("# generated: len seed hash (XXH3_64bits_withSeed, scalar)\n");
  for (size_t s = 0; s < kat_nseeds; s++) for (size_t l = 0; l < kat_nlens; l++)
    printf("K %zu %016llx %016llx\n", kat_lens[l], (unsigned long long)kat_seeds[s], (unsigned long long)XXH3_64bits_withSeed(buf, kat_lens[l], kat_seeds[s]));
  return 0;
}
