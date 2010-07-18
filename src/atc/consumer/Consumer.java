package atc.consumer;

public interface Consumer<T> {
    public void begin();
    public void send(T obj);
    public void end();
}
