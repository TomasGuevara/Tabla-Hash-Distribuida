public class CacheEntry {
    String value;
    long expirationTime;

    public CacheEntry(String value, long expirationTime) {
        this.value = value;
        this.expirationTime = expirationTime;
    }

    public boolean isExpired() {
        return System.currentTimeMillis() > expirationTime;
    }
}
