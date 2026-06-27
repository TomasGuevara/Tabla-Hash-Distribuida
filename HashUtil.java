import java.math.BigInteger;
import java.security.MessageDigest;

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
