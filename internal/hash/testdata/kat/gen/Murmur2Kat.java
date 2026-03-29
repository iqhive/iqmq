/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// Modified: extracted utilities into a standalone vector generator and added input/partition cases.

import java.nio.charset.StandardCharsets;

/** Kafka 4.1.0 utilities with a standalone known-answer generator harness. */
public class Murmur2Kat {

    @SuppressWarnings("fallthrough")
    public static int murmur2(final byte[] data) {
        int length = data.length;
        int seed = 0x9747b28c;
        // 'm' and 'r' are mixing constants generated offline.
        // They're not really 'magic', they just happen to work well.
        final int m = 0x5bd1e995;
        final int r = 24;

        // Initialize the hash to a random value
        int h = seed ^ length;
        int length4 = length / 4;

        for (int i = 0; i < length4; i++) {
            final int i4 = i * 4;
            int k = (data[i4 + 0] & 0xff) + ((data[i4 + 1] & 0xff) << 8) + ((data[i4 + 2] & 0xff) << 16) + ((data[i4 + 3] & 0xff) << 24);
            k *= m;
            k ^= k >>> r;
            k *= m;
            h *= m;
            h ^= k;
        }

        // Handle the last few bytes of the input array
        switch (length % 4) {
            case 3:
                h ^= (data[(length & ~3) + 2] & 0xff) << 16;
            case 2:
                h ^= (data[(length & ~3) + 1] & 0xff) << 8;
            case 1:
                h ^= data[length & ~3] & 0xff;
                h *= m;
        }

        h ^= h >>> 13;
        h *= m;
        h ^= h >>> 15;

        return h;
    }

    public static int toPositive(int number) {
        return number & 0x7fffffff;
    }

    static final int[] LENS = {0,1,2,3,4,5,6,7,8,9,11,12,13,15,16,17,18,23,24,25,31,32,33,40,47,48,49,55,56,63,64,65,80,95,96,97,111,112,127,128,129,130,143,144,145,160,176,192,195,200,208,224,239,240,241,255,256,257,300,320,383,384,385,403,511,512,513,767,768,1000,1023,1024,1025,1087,1088,1089,2047,2048,2049,2099,2240,2367,3000,4095,4096};
    static final int[] PARTS = {1, 2, 3, 7, 12, 50, 64, 100, 1000, 65536};

    public static void main(String[] args) {
        String[] known = {"21", "foobar", "a-little-bit-long-string", "a-little-bit-longer-string", "lkjh234lh9fiuh90y23oiuhsafujhadof229phr9h19h89h8", "abc"};
        System.out.println("# UtilsTest.testMurmur2 strings: V <string> <int>");
        for (String s : known) System.out.println("V " + s + " " + murmur2(s.getBytes(StandardCharsets.UTF_8)));
        byte[] buf = new byte[4096];
        long g = 2654435761L;
        for (int i = 0; i < buf.length; i++) { buf[i] = (byte) (g >>> 56); g *= 0x9E3779B185EBCA8DL; }
        System.out.println("# generated: K <len> <murmur2 as uint32 hex> <partition for each of PARTS>");
        for (int len : LENS) {
            byte[] key = java.util.Arrays.copyOf(buf, len);
            int h = murmur2(key);
            StringBuilder sb = new StringBuilder();
            sb.append("K ").append(len).append(' ').append(String.format("%08x", h));
            for (int p : PARTS) sb.append(' ').append(toPositive(h) % p);
            System.out.println(sb);
        }
    }
}
