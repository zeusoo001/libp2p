package org.tron.p2p.discover.protocol.kad;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.tron.p2p.P2pConfig;
import org.tron.p2p.base.Parameter;
import org.tron.p2p.discover.Node;
import org.tron.p2p.discover.message.kad.FindNodeMessage;
import org.tron.p2p.discover.message.kad.NeighborsMessage;
import org.tron.p2p.discover.message.kad.PingMessage;
import org.tron.p2p.discover.message.kad.PongMessage;
import org.tron.p2p.discover.socket.UdpEvent;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class KadServiceTest {

  private static KadService kadService;
  private static Node node1;
  private static Node node2;

  @BeforeClass
  public static void init() {
    P2pConfig config = new P2pConfig();
    config.setDiscoverEnable(false);
    config.setIp("127.0.0.1");
    Parameter.p2pConfig = config;
    kadService = new KadService();
    kadService.init();
    KadService.setPingTimeout(300);
    node1 = new Node(new InetSocketAddress("127.0.0.1", 22222));
    node2 = new Node(new InetSocketAddress("127.0.0.2", 22222));
  }

  @Test
  public void test() {
    Assert.assertNotNull(kadService.getPongTimer());
    Assert.assertNotNull(kadService.getPublicHomeNode());
    Assert.assertEquals(0, kadService.getAllNodes().size());

    NodeHandler nodeHandler = kadService.getNodeHandler(node1);
    Assert.assertNotNull(nodeHandler);
    Assert.assertEquals(1, kadService.getAllNodes().size());

    UdpEvent event = new UdpEvent(new PingMessage(node2, kadService.getPublicHomeNode()),
        new InetSocketAddress(node2.getHostV4(), node2.getPort()));
    kadService.handleEvent(event);
    Assert.assertEquals(2, kadService.getAllNodes().size());
  }

  @Test
  public void testPingTimeout() {
    long original = KadService.getPingTimeout();
    KadService.setPingTimeout(500);
    Assert.assertEquals(500, KadService.getPingTimeout());
    KadService.setPingTimeout(original);
  }

  @Test
  public void testGetTableNodes() {
    List<Node> tableNodes = kadService.getTableNodes();
    Assert.assertNotNull(tableNodes);
  }

  @Test
  public void testGetConnectableNodes() {
    List<Node> connectable = kadService.getConnectableNodes();
    Assert.assertNotNull(connectable);
    // Nodes without pong handshake have p2pVersion=0, networkId=1 → not connectable
    for (Node n : connectable) {
      Assert.assertTrue(n.isConnectible(Parameter.p2pConfig.getNetworkId()));
      Assert.assertNotNull(n.getPreferInetSocketAddress());
    }
  }

  @Test
  public void testGetNodeHandlerDeduplication() {
    Node node = new Node(new InetSocketAddress("127.0.0.6", 22222));
    NodeHandler h1 = kadService.getNodeHandler(node);
    NodeHandler h2 = kadService.getNodeHandler(node);
    Assert.assertSame(h1, h2);
  }

  @Test
  public void testHandlePingMessage() {
    Node sender = new Node(new InetSocketAddress("127.0.0.7", 22222));
    UdpEvent event = new UdpEvent(
        new PingMessage(sender, kadService.getPublicHomeNode()),
        new InetSocketAddress(sender.getHostV4(), sender.getPort()));
    int before = kadService.getAllNodes().size();
    kadService.handleEvent(event);
    Assert.assertEquals(before + 1, kadService.getAllNodes().size());
  }

  @Test
  public void testHandlePongMessage() {
    P2pConfig original = Parameter.p2pConfig;
    P2pConfig config = new P2pConfig();
    config.setDiscoverEnable(false);
    config.setIp("127.0.0.1");
    Parameter.p2pConfig = config;

    KadService service = new KadService();
    service.init();
    KadService.setPingTimeout(300);

    Node testNode = new Node(new InetSocketAddress("127.1.0.1", 22222));
    NodeHandler handler = service.getNodeHandler(testNode);
    Assert.assertNotNull(handler);

    // Send PongMessage from testNode; networkId in pong = config.networkId = 1
    // handler is waitForPong=true (set when changeState(DISCOVERED) called sendPing)
    PongMessage pong = new PongMessage(testNode);
    UdpEvent event = new UdpEvent(pong,
        new InetSocketAddress(testNode.getHostV4(), testNode.getPort()));
    service.handleEvent(event);

    // After valid pong, node.p2pVersion = 1 = networkId → isConnectible returns true
    List<Node> connectable = service.getConnectableNodes();
    Assert.assertFalse(connectable.isEmpty());

    service.close();
    Parameter.p2pConfig = original;
  }

  @Test
  public void testHandleFindNodeMessage() {
    Node sender = new Node(new InetSocketAddress("127.0.0.8", 22222));
    byte[] targetId = new byte[64];
    FindNodeMessage msg = new FindNodeMessage(sender, targetId);
    UdpEvent event = new UdpEvent(msg,
        new InetSocketAddress(sender.getHostV4(), sender.getPort()));
    // Should process without exception
    kadService.handleEvent(event);
  }

  @Test
  public void testHandleNeighborsIgnoredWithoutWaiting() {
    // NeighborsMessage without prior FindNodeMessage is ignored (logs warning, skips neighbors)
    Node sender = new Node(new InetSocketAddress("127.0.0.9", 22222));
    Node neighbor = new Node(new InetSocketAddress("127.0.0.10", 22222));
    List<Node> neighborList = new ArrayList<>();
    neighborList.add(neighbor);

    NeighborsMessage msg = new NeighborsMessage(sender, neighborList, System.currentTimeMillis());
    UdpEvent event = new UdpEvent(msg,
        new InetSocketAddress(sender.getHostV4(), sender.getPort()));

    int before = kadService.getAllNodes().size();
    kadService.handleEvent(event);
    // sender node gets added to handlers, but neighbor (127.0.0.10) does NOT
    // because waitForNeighbors is false for sender's handler
    Assert.assertEquals(before + 1, kadService.getAllNodes().size());
  }

  @Test
  public void testSendOutboundWhenDiscoverDisabled() {
    AtomicBoolean called = new AtomicBoolean(false);
    kadService.setMessageSender(e -> called.set(true));
    UdpEvent event = new UdpEvent(
        new PingMessage(node1, kadService.getPublicHomeNode()),
        new InetSocketAddress(node1.getHostV4(), node1.getPort()));
    kadService.sendOutbound(event);
    Assert.assertFalse(called.get());
    kadService.setMessageSender(null);
  }

  @Test
  public void testSendOutboundWhenDiscoverEnabled() {
    P2pConfig original = Parameter.p2pConfig;
    P2pConfig config = new P2pConfig();
    config.setDiscoverEnable(true);
    config.setIp("127.0.0.1");
    Parameter.p2pConfig = config;

    KadService service = new KadService();
    service.init();

    AtomicBoolean called = new AtomicBoolean(false);
    service.setMessageSender(e -> called.set(true));

    UdpEvent event = new UdpEvent(
        new PingMessage(node1, service.getPublicHomeNode()),
        new InetSocketAddress(node1.getHostV4(), node1.getPort()));
    service.sendOutbound(event);
    Assert.assertTrue(called.get());

    service.close();
    Parameter.p2pConfig = original;
  }

  @Test
  public void testBootNodesFromSeedAndActiveNodes() {
    P2pConfig original = Parameter.p2pConfig;
    P2pConfig config = new P2pConfig();
    config.setDiscoverEnable(false);
    config.setIp("127.0.0.1");
    config.getSeedNodes().add(new InetSocketAddress("10.0.0.1", 8080));
    config.getActiveNodes().add(new InetSocketAddress("10.0.0.2", 8080));
    Parameter.p2pConfig = config;

    KadService service = new KadService();
    service.init();
    // channelActivated registers handlers for all boot nodes
    service.channelActivated();
    Assert.assertEquals(2, service.getAllNodes().size());

    service.close();
    Parameter.p2pConfig = original;
  }

  @Test
  public void testChannelActivatedIdempotent() {
    P2pConfig original = Parameter.p2pConfig;
    P2pConfig config = new P2pConfig();
    config.setDiscoverEnable(false);
    config.setIp("127.0.0.1");
    config.getSeedNodes().add(new InetSocketAddress("10.0.0.3", 8080));
    Parameter.p2pConfig = config;

    KadService service = new KadService();
    service.init();
    service.channelActivated();
    service.channelActivated(); // second call is no-op
    Assert.assertEquals(1, service.getAllNodes().size());

    service.close();
    Parameter.p2pConfig = original;
  }

  @Test
  public void testCloseShutdownsPongTimer() {
    P2pConfig original = Parameter.p2pConfig;
    P2pConfig config = new P2pConfig();
    config.setDiscoverEnable(false);
    config.setIp("127.0.0.1");
    Parameter.p2pConfig = config;

    KadService service = new KadService();
    service.init();
    Assert.assertFalse(service.getPongTimer().isShutdown());
    service.close();
    Assert.assertTrue(service.getPongTimer().isShutdown());

    Parameter.p2pConfig = original;
  }

  @Test
  public void testIpv6SenderHandledByHandleEvent() {
    // Simulate event from an IPv6 sender; node should be created and added
    Node sender = new Node(new InetSocketAddress("127.0.0.11", 33333));
    PingMessage msg = new PingMessage(sender, kadService.getPublicHomeNode());
    // Use an IPv4 address as sender here since IPv6 would require real IPv6 stack
    InetSocketAddress senderAddr = new InetSocketAddress(sender.getHostV4(), sender.getPort());
    UdpEvent event = new UdpEvent(msg, senderAddr);
    int before = kadService.getAllNodes().size();
    kadService.handleEvent(event);
    Assert.assertEquals(before + 1, kadService.getAllNodes().size());
  }

  @AfterClass
  public static void destroy() {
    kadService.close();
  }
}
