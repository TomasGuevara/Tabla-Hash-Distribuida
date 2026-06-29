import java.math.BigInteger;
import java.security.MessageDigest;

/* Utility class providing cryptographic hash functions
 * used by the Chord implementation.
 *
 * In Chord, both node identifiers and keys are mapped
 * into the identifier space using SHA-1.
 *
 * Although the original Chord protocol uses a 160-bit
 * identifier space, this implementation may reduce the
 * effective ring size by applying a modulo operation
 * after hashing.
 */
public class HashUtil {

    public static BigInteger sha1(String value) {

        try {

            MessageDigest md =
                MessageDigest.getInstance("SHA-1");

            byte[] hash =
                md.digest(value.getBytes());

            return new BigInteger(1, hash);

        } catch (Exception e) {

            throw new RuntimeException(e);
        }
    }
}
