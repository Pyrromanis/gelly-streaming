package HyperBall;

import net.agkn.hll.HLL;
import org.apache.flink.shaded.curator.org.apache.curator.shaded.com.google.common.hash.HashFunction;
import org.apache.flink.shaded.curator.org.apache.curator.shaded.com.google.common.hash.Hashing;

import java.util.stream.LongStream;


import org.assertj.core.data.Offset;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;


public class hyperloglog {
    public static void main(String[] args){
        HashFunction hashFunction = Hashing.murmur3_128();
        long numberOfElements = 100_000_000;
        long toleratedDifference = 1_000_000;
        HLL hll = new HLL(14, 5);   //log2m     =no of regs used by hll
                                                   //regwidth  =bits per reg

        LongStream.range(0, numberOfElements).forEach(element -> {
                    long hashedValue = hashFunction.newHasher().putLong(element).hash().asLong();
                    hll.addRaw(hashedValue);
                }
        );

        long cardinality = hll.cardinality();
        assertThat(cardinality).isCloseTo(numberOfElements, Offset.offset(toleratedDifference));
        System.out.println(cardinality);
    }


}
