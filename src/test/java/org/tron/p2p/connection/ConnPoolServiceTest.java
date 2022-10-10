package org.tron.p2p.connection;


import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.tron.p2p.P2pConfig;
import org.tron.p2p.base.Parameter;
import org.tron.p2p.connection.business.pool.ConnPoolService;
import org.tron.p2p.discover.Node;
import org.tron.p2p.discover.NodeManager;

public class ConnPoolServiceTest {

  private static String localIp = "127.0.0.1";
  private static int port = 10000;

  @Before
  public void init() {
    Parameter.p2pConfig = new P2pConfig();
    Parameter.p2pConfig.setDiscoverEnable(false);

    NodeManager.init();
    ChannelManager.init();
  }

  private void clearChannels() {
    ChannelManager.getChannels().clear();
    ChannelManager.getBannedNodes().cleanUp();
  }

  @Test
  public void getNodes_chooseHomeNode() {
    InetSocketAddress localAddress = new InetSocketAddress(Parameter.p2pConfig.getIp(),
        Parameter.p2pConfig.getPort());
    Node localNode = new Node(localAddress);
    List<Node> connectableNodes = new ArrayList<>();
    connectableNodes.add(localNode);

    ConnPoolService connPoolService = new ConnPoolService();
    List<Node> nodes = connPoolService.getNodes(new HashSet<>(), connectableNodes, 1);

    Assert.assertEquals(0, nodes.size());
  }

  @Test
  public void getNodes_orderByUpdateTimeDesc() {
    clearChannels();
    InetSocketAddress localAddress1 = new InetSocketAddress(localIp, port);
    Node node1 = new Node(localAddress1);
    try {
      Thread.sleep(100);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    InetSocketAddress localAddress2 = new InetSocketAddress(localIp, port + 1);
    Node node2 = new Node(localAddress2);

    Assert.assertTrue(node1.getUpdateTime() < node2.getUpdateTime());

    List<Node> connectableNodes = new ArrayList<>();
    connectableNodes.add(node1);
    connectableNodes.add(node2);
    ConnPoolService connPoolService = new ConnPoolService();
    List<Node> nodes = connPoolService.getNodes(new HashSet<>(), connectableNodes, 2);
    Assert.assertEquals(2, nodes.size());
    Assert.assertTrue(nodes.get(0).getUpdateTime() > nodes.get(1).getUpdateTime());

    int limit = 1;
    List<Node> nodes2 = connPoolService.getNodes(new HashSet<>(), connectableNodes, limit);
    Assert.assertEquals(limit, nodes2.size());
  }

  @Test
  public void getNodes_banNode() {
    clearChannels();
    InetSocketAddress inetSocketAddress = new InetSocketAddress(localIp, port);
    long banTime = 1_000L;
    ChannelManager.banNode(inetSocketAddress.getAddress(), banTime);
    Node node = new Node(inetSocketAddress);
    List<Node> connectableNodes = new ArrayList<>();
    connectableNodes.add(node);

    ConnPoolService connPoolService = new ConnPoolService();
    List<Node> nodes = connPoolService.getNodes(new HashSet<>(), connectableNodes, 1);
    Assert.assertEquals(0, nodes.size());

    try {
      Thread.sleep(banTime);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    nodes = connPoolService.getNodes(new HashSet<>(), connectableNodes, 1);
    Assert.assertEquals(1, nodes.size());
  }

  @Test
  public void getNodes_nodeInUse() {
    clearChannels();
    InetSocketAddress inetSocketAddress = new InetSocketAddress(localIp, port);
    Node node = new Node(inetSocketAddress);
    List<Node> connectableNodes = new ArrayList<>();
    connectableNodes.add(node);

    Set<String> nodesInUse = new HashSet<>();
    nodesInUse.add(node.getHexId());
    ConnPoolService connPoolService = new ConnPoolService();
    List<Node> nodes = connPoolService.getNodes(nodesInUse, connectableNodes, 1);
    Assert.assertEquals(0, nodes.size());
  }

  @After
  public void destroy() {
    NodeManager.close();
    ChannelManager.close();
  }
}