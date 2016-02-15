package BPlusTree;

import java.util.*;

public class FindKeyIndex<K extends Comparable<K>, T> {
  Node<K,T> currentNode;
  K key;
	
  public FindKeyIndex(Node<K,T> currentNode, K key) {
    this.currentNode = currentNode;
    this.key = key;
  }
	
  /**
   * find the corresponding index for the key
   *
   * @param node the node that is in searching
   * @param key 
   * @return index of the key in the key
   */
  public int find() {
    int index = Collections.binarySearch(currentNode.keys, key) + 1; 
    // if the key is not accurately matched, let it be one pointer before the insertion point
    
    return index < 0 ? -(index) : index; // -(insertion point) - 1
  }
}
