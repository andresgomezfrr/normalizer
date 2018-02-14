package rb.ks.builder.bootstrap;

public abstract class ThreadBootstraper extends Thread implements Bootstraper {

    @Override
    public void interrupt() {
        close();
    }
}
