/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package leader;

import com.google.common.collect.Lists;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * An example leader selector that holds a few clients in threads and interrupts them after small time intervals to
 * demonstrate Zookeeper's ability to elect a new leader when one goes down.
 */
public class LeaderSelectorExample  extends LeaderSelectorListenerAdapter implements Closeable
{
    private static final int CLIENT_QTY = 10;

    public static final String PATH = "/amf/leader";

    private final String name;
    private final LeaderSelector leaderSelector;
    private final AtomicInteger leaderCount = new AtomicInteger();

    public static void main(String[] args) throws Exception {

        String zookeepers;
        if(args.length == 1) {
            zookeepers = args[0];
        } else {
            System.out.println("Error: supply zookeeper connection string as argument.");
            return;
        }

        System.out.println("Create " + CLIENT_QTY + " clients, have each negotiate for leadership and then wait a random number of seconds before letting another leader election occur.");
        System.out.println("Notice that leader election is fair: all clients will become leader and will do so the same number of times.");

        List<CuratorFramework>  clients = Lists.newArrayList();
        List<LeaderSelectorExample> examples = Lists.newArrayList();
        TestingServer server = new TestingServer();

        try {
            for (int i = 0; i < CLIENT_QTY; ++i) {
                CuratorFramework    client = CuratorFrameworkFactory.newClient(zookeepers, new ExponentialBackoffRetry(1000, 3));
                clients.add(client);

                LeaderSelectorExample example = new LeaderSelectorExample(client, PATH, "Client #" + i);
                examples.add(example);

                client.start();
                example.start();
            }

            System.out.println("Press enter/return to quit\n");
            new BufferedReader(new InputStreamReader(System.in)).readLine();
        }
        finally {
            System.out.println("Shutting down...");

            for (LeaderSelectorExample exampleClient : examples)
                CloseableUtils.closeQuietly(exampleClient);

            for ( CuratorFramework client : clients )
                CloseableUtils.closeQuietly(client);

            CloseableUtils.closeQuietly(server);
        }
    }

    public LeaderSelectorExample(CuratorFramework client, String path, String name) {
        this.name = name;

        // create a leader selector using the given path for management
        // all participants in a given leader selection must use the same path
        // ExampleClient here is also a LeaderSelectorListener but this isn't required
        leaderSelector = new LeaderSelector(client, path, this);

        // for most cases you will want your instance to requeue when it relinquishes leadership
        leaderSelector.autoRequeue();
    }

    public void start() throws IOException {
        // the selection for this instance doesn't start until the leader selector is started
        // leader selection is done in the background so this call to leaderSelector.start() returns immediately
        leaderSelector.start();
    }

    @Override
    public void close() throws IOException {
        leaderSelector.close();
    }

    @Override
    public void takeLeadership(CuratorFramework client) throws Exception {
        // we are now the leader. This method should not return until we want to relinquish leadership

        final int waitSeconds = (int)(5 * Math.random()) + 1;

        System.out.println(name + " is now the leader. Waiting " + waitSeconds + " seconds...");
        System.out.println(name + " has been leader " + leaderCount.getAndIncrement() + " time(s) before.");
        try {
            Thread.sleep(TimeUnit.SECONDS.toMillis(waitSeconds));
        }
        catch (InterruptedException e) {
            System.err.println(name + " was interrupted.");
            Thread.currentThread().interrupt();
        }
        finally {
            System.out.println(name + " relinquishing leadership.\n");
        }

        Thread.currentThread().interrupt();
    }
}
