package leader;


import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.recipes.leader.Participant;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;

import static leader.LeaderSelectorExample.PATH;

/**
 * This is a client that registers a callback to be notified when a leader changes. This allows it to act
 */
public class LeaderChangedClient implements LeaderSelectorListener {

    public static void main(String args[]) throws Exception {

        String zookeepers;
        if(args.length == 1) {
            zookeepers = args[0];
        } else {
            System.out.println("Error: supply zookeeper connection string as argument.");
            return;
        }

        CuratorFramework client = CuratorFrameworkFactory.newClient(zookeepers, new ExponentialBackoffRetry(1000, 3));
        client.start();

        final LeaderSelector leaderSelector = new LeaderSelector(client, PATH, new LeaderChangedClient());


        PathChildrenCache cache = new PathChildrenCache(client, PATH, false);
        cache.getListenable().addListener(new PathChildrenCacheListener() {
            Participant currentLeader = null;

            @Override
            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {

                if(event.getType() == PathChildrenCacheEvent.Type.CHILD_ADDED) {

                    Participant leader = leaderSelector.getLeader();

                    if(currentLeader == null || (currentLeader != null && !currentLeader.equals(leader))) {
                        currentLeader = leaderSelector.getLeader();
                        System.out.println("Leader changed. New leader = " + currentLeader);
                    }

                    /**
                     * TODO: Perform some work to reconfigure the system because we found a new leader....
                     */

                } else if(event.getType() == PathChildrenCacheEvent.Type.CHILD_REMOVED) {
                    Participant leader = leaderSelector.getLeader();
                    if(!leader.isLeader() && currentLeader != null) {
                        currentLeader = null;
                        System.out.println("We currently don't have any leaders.");
                    }

                }
            }
        });

        cache.start();

        while(true){}
    }


    @Override
    public void takeLeadership(CuratorFramework client) throws Exception {
        // this method won't be getting called since we aren't joining the quorum
    }

    @Override
    public void stateChanged(CuratorFramework client, ConnectionState newState) {
        // state change of our own client is never good after connecting.
        if(newState != ConnectionState.CONNECTED)
            System.out.println(newState);
    }
}
