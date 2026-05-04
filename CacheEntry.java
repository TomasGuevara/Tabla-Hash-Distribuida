public class CacheEntry {
    String value;
    long expirationTime;

    /*
     * A constructor that initializes the value and its expiration time.
     */
    public CacheEntry(String value, long expirationTime) {
        this.value = value;
        this.expirationTime = expirationTime;
    }

    /*
     * Checks whether the cache entry has expired. 
     */
    public boolean isExpired() {
        return System.currentTimeMillis() > expirationTime;
    }
}
