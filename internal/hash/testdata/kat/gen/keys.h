/*
 * xxHash - Extremely Fast Hash algorithm
 * Header File
 * Copyright (C) 2012-2023 Yann Collet
 *
 * BSD 2-Clause License (https://www.opensource.org/licenses/bsd-license.php)
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 *    * Redistributions of source code must retain the above copyright
 *      notice, this list of conditions and the following disclaimer.
 *    * Redistributions in binary form must reproduce the above
 *      copyright notice, this list of conditions and the following disclaimer
 *      in the documentation and/or other materials provided with the
 *      distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * You can contact the author at:
 *   - xxHash homepage: https://www.xxhash.com
 *   - xxHash source repository: https://github.com/Cyan4973/xxHash
 */

/* Deterministic key material shared by every KAT generator: the xxHash
   sanity-buffer PRNG (byteGen starts at PRIME32, multiplied by PRIME64,
   top byte emitted). Keys are prefixes buf[0:len]. */
#include <stdint.h>
#include <stddef.h>
#define KAT_BUF 4096
static void kat_fill(uint8_t *buf, size_t len) {
  uint64_t g = 2654435761ULL;
  for (size_t i = 0; i < len; i++) { buf[i] = (uint8_t)(g >> 56); g *= 11400714785074694797ULL; }
}
static const size_t kat_lens[] = {0,1,2,3,4,5,6,7,8,9,11,12,13,15,16,17,18,23,24,25,31,32,33,40,47,48,49,55,56,63,64,65,80,95,96,97,111,112,127,128,129,130,143,144,145,160,176,192,195,200,208,224,239,240,241,255,256,257,300,320,383,384,385,403,511,512,513,767,768,1000,1023,1024,1025,1087,1088,1089,2047,2048,2049,2099,2240,2367,3000,4095,4096};
static const size_t kat_nlens = sizeof(kat_lens)/sizeof(kat_lens[0]);
static const uint64_t kat_seeds[] = {0ULL, 1ULL, 0x9747b28cULL, 11400714785074694797ULL, 0xFFFFFFFFFFFFFFFFULL, 0x0123456789abcdefULL};
static const size_t kat_nseeds = sizeof(kat_seeds)/sizeof(kat_seeds[0]);
