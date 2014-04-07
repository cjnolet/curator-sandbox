package leader;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;

public class MyStateListener implements ConnectionStateListener {
    @Override
    public void stateChanged(CuratorFramework client, ConnectionState newState) {
        System.out.println("New State: " + newState);
    }
}
