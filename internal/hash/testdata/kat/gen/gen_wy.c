#include <stdio.h>
#include <string.h>
#include "wyhash.h"
#include "keys.h"
int main(void) {
  const char *msgs[] = {"", "a", "abc", "message digest", "abcdefghijklmnopqrstuvwxyz",
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789",
    "12345678901234567890123456789012345678901234567890123456789012345678901234567890"};
  printf("# upstream test_vector.cpp (seed = index)\n");
  for (size_t i = 0; i < 7; i++) printf("V %zu %016llx\n", i, (unsigned long long)wyhash(msgs[i], strlen(msgs[i]), i, _wyp));
  uint8_t buf[KAT_BUF]; kat_fill(buf, KAT_BUF);
  printf("# generated: len seed hash\n");
  for (size_t s = 0; s < kat_nseeds; s++) for (size_t l = 0; l < kat_nlens; l++)
    printf("K %zu %016llx %016llx\n", kat_lens[l], (unsigned long long)kat_seeds[s], (unsigned long long)wyhash(buf, kat_lens[l], kat_seeds[s], _wyp));
  return 0;
}
