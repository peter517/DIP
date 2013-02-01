package test;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

public class ZookeeperTest {
	
	public static void testCreateNode(ZooKeeper zk, String path, byte[] datas){
		try {
			zk.create(path, datas, Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static byte[] testGetNodeData(ZooKeeper zk, String path){
		try {
			return zk.getData(path, false, null);
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return null;
	}
	
	public static void testUpdateNodeData(ZooKeeper zk, String path, byte[] datas){
		try {
			zk.setData(path, datas, -1);
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void testDeleteNode(ZooKeeper zk, String path){
		try {
			zk.delete(path, 1);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void testGetAllChildrenNodes(ZooKeeper zk, String path){
		try {
			List<String> children = zk.getChildren(path, false);
			if(children == null || children.size() == 0){
				byte[] data = testGetNodeData(zk,path);
				if(data == null || data.length == 0){
					System.out.println(path);
					return;
				}
				short version = Bytes.toShort(data);
			    if (version == 0) {
			    	int length = data.length - Bytes.SIZEOF_SHORT;
			    	System.out.println(path + ": " + new String(data,Bytes.SIZEOF_SHORT, length));
			    }
			    else System.out.println(path + ": " + new String(data, 5, data.length-5));
			    
				
			}
			else for(String child: children){
				//System.out.println(path);
				if(path.endsWith("/")) testGetAllChildrenNodes(zk, path+child);
				else testGetAllChildrenNodes(zk, path+"/"+child);
			}
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static Stat testExistNode(ZooKeeper zk, String path){
		try {
			return zk.exists(path, false);
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return null;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			ZooKeeper zk=new ZooKeeper("211.69.207.156:2181",500000,new Watcher() {

				public void process(WatchedEvent event) {
					System.out.println("type:" + event.getType());
				}
			});			
			
//			testCreateNode(zk,"/idc","test node data".getBytes());
			//testDeleteNode(zk,"/idc/hbase");
			
			//testDeleteNode(zk,"/idc-hbase");
			//Stat s = testExistNode(zk,"/idc-hbase/table/test-scores");
			//System.out.println(s);
			//System.out.println(new String(testGetNodeData(zk,"/idc-hbase/table/test-scores")));
			
			testGetAllChildrenNodes(zk, "/");
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			//testCreateNode(zk,"/idc/hbase","test node data".getBytes());
			//System.out.println(new String(testGetNodeData(zk,"/testRootPath/test1")));

			try {
				zk.close();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		
	}

}
