package BPlusTree;

import java.util.Collections;
import java.util.Map.Entry;

public class Search<K extends Comparable<K>, T> extends BPlusTree<K, T> {
	
  public Entry<K, Node<K,T>> newchildentry;  // the helper entry for insert
  public Node<K,T> root;
  public static final int D = 2;
	  
  public Search(Entry<K, Node<K,T>> newchildentry, Node<K,T> root) {
    this.newchildentry = newchildentry;
    this.root = root;
  }
	
  /**
   * Search the value for a specific key
   * Each node has multiple keys, and each key corresponds to one value
   * 
   * @param key
   * @return value
   */
  public T search(K key) {
    if (root == null || key == null) {
      return null;
    }

    LeafNode<K,T> targetNode = searchLeafNode(key, root);
    int index = Collections.binarySearch(targetNode.keys, key);

    if (index < 0) { // not in the list
      return null;
    }

    return targetNode.values.get(index);
  }
	  
  /**
   * Helper method for search the leaf node which may contain the key
   *
   * @param currentNode the node that is in searching
   * @return leafnode with the key
   */
  public LeafNode<K,T> searchLeafNode(K key, Node<K,T> currentNode) {
    if (currentNode.isLeafNode) {
      return (LeafNode<K,T>)currentNode;
    }

    int index = new FindKeyIndex(currentNode, key).find();
    Node<K,T> child = ((IndexNode<K,T>)currentNode).children.get(index);

    return searchLeafNode(key, child);
  }
}
