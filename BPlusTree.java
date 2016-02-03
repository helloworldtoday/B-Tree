package BPlusTree;

import java.util.AbstractMap;
import java.util.AbstractMap.SimpleEntry;
import java.util.Map.Entry;
import java.util.*;

/**
 * BPlusTree Class Assumptions: 
 * 1. No duplicate keys inserted 
 * 2. Order D: D <= number of keys in a node <= 2*D 
 * 3. All keys are non-negative
 */
public class BPlusTree<K extends Comparable<K>, T> {

  private Entry<K, Node<K,T>> newchildentry;  // the helper entry for insert
  public Node<K,T> root;
  public static final int D = 2;

  // search
  public T search(K key) {
    return (T) new Search(newchildentry, root).search(key);
  }

	/**
	 * TODO Insert a key/value pair into the BPlusTree
	 * 
	 * @param key
	 * @param value
	 */
	public void insert(K key, T value) {
		Entry<K,Node<K,T>> entry = new AbstractMap.SimpleEntry<K,Node<K,T>>(key, new LeafNode<K, T>(key,value));
        if (root == null)
            root = entry.getValue();
        Entry<K,Node<K,T>> newChildEntry = insertHelper(root,entry,null);
        if (newChildEntry == null)
            return;
        else {
            IndexNode<K, T> newRoot = new IndexNode(newChildEntry.getKey(), root, newChildEntry.getValue());
            root = newRoot;
            return;
        }
	}
	
	private Entry<K, Node<K,T>> insertHelper(Node<K, T> node,Entry<K, Node<K,T>> entry, Entry<K, Node<K,T>> newChildEntry) {
        if (!node.isLeafNode){
            IndexNode<K, T> idx = (IndexNode)node;
            int i = 0;
            while (i < node.keys.size()) {
                if (entry.getKey().compareTo(node.keys.get(i)) < 0)
                    break;
                i++;
            }
            newChildEntry = insertHelper((Node)idx.children.get(i), entry, newChildEntry);
            if (newChildEntry == null) {
                return newChildEntry;
            } else {
                int j = 0;
                while (j < idx.keys.size()){
                    if (newChildEntry.getKey().compareTo(node.keys.get(j)) < 0)
                        break;
                    j++;
                }

                idx.insertSorted(newChildEntry,j);

                if (!idx.isOverflowed()){
                    newChildEntry = null;
                    return newChildEntry;
                } else {
                    newChildEntry = splitIndexNode(idx);
                    if (idx == root){
                        IndexNode<K, T> newRoot = new IndexNode(newChildEntry.getKey(),root,newChildEntry.getValue());
                        root = newRoot;
                        newChildEntry = null;
                        return newChildEntry;
                    }
                    return newChildEntry;
                }
            }
        } else {
            LeafNode<K, T> lf = (LeafNode)node;
            LeafNode<K, T> InsertLeaf = (LeafNode)entry.getValue();

            lf.insertSorted(entry.getKey(),InsertLeaf.values.get(0));

            if (!lf.isOverflowed()){
                newChildEntry = null;
                return newChildEntry;
            } else {
                newChildEntry = splitLeafNode(lf);
                if (lf == root){
                    IndexNode<K, T> newRoot = new IndexNode(newChildEntry.getKey(),lf,newChildEntry.getValue());
                    root = newRoot;
                    newChildEntry = null;
                    return newChildEntry;
                }
                return newChildEntry;
            }
        }
    }
	
	/**
	 * TODO Split a leaf node and return the new right node and the splitting
	 * key as an Entry<slitingKey, RightNode>
	 * 
	 * @param leaf
	 * @return the key/node pair as an Entry
	 */
	public Entry<K, Node<K,T>> splitLeafNode(LeafNode<K,T> leaf) {
		ArrayList<K> rightKeys = new ArrayList<K>();
        ArrayList<T> rightValues = new ArrayList<T>();
        K splittingKey = leaf.keys.get(D);

        while (leaf.keys.size() > D){
            rightKeys.add(leaf.keys.get(D));
            leaf.keys.remove(D);
            rightValues.add(leaf.values.get(D));
            leaf.values.remove(D);
        }

        LeafNode<K, T> rightNode = new LeafNode<>(rightKeys, rightValues);
        LeafNode<K, T> Tmp = leaf.nextLeaf;
        leaf.nextLeaf = rightNode;
        leaf.nextLeaf.previousLeaf = rightNode;
        rightNode.previousLeaf = leaf;
        rightNode.nextLeaf = Tmp;

        Entry<K,Node<K,T>> entry = new AbstractMap.SimpleEntry<K,Node<K,T>>(splittingKey, rightNode);
		return entry;
	}

	/**
	 * TODO split an indexNode and return the new right node and the splitting
	 * key as an Entry<slitingKey, RightNode>
	 * 
	 * @param index
	 * @return new key/node pair as an Entry
	 */
	public Entry<K, Node<K,T>> splitIndexNode(IndexNode<K,T> index) {
		K splittingKey = index.keys.get(D);
        index.keys.remove(D);

        ArrayList<K> RightKey = new ArrayList<K>();
        ArrayList<Node<K,T>> RightChildren = new ArrayList<Node<K, T>>();

        RightChildren.add(index.children.get(D+1));
        index.children.remove(D+1);

        while (index.keys.size() > D){
            RightKey.add(index.keys.get(D));
            index.keys.remove(D);
            RightChildren.add(index.children.get(D + 1));
            index.children.remove(D + 1);
        }

        IndexNode<K, T> Right = new IndexNode<>(RightKey, RightChildren);
        Entry<K,Node<K,T>> entry = new AbstractMap.SimpleEntry<K,Node<K,T>>(splittingKey, Right);
		return entry;
	}

	/**
	 * TODO Delete a key/value pair from this B+Tree
	 * 
	 * @param key
	 */
	public void delete(K key) {
		if (root == null) {
			return;
		} else {
			delete(null, root, key, 0);
		}
	}
	
	private int delete(IndexNode<K, T> parentNode, Node<K,T> currentNode, K key, int currentIndex) {
		if (!currentNode.isLeafNode) {  // condition 1: index node
			IndexNode<K, T> currIndexNode = (IndexNode<K, T>)currentNode;
			int childIndex = findKeyIndex(currIndexNode, key);
			Node<K,T> child = currIndexNode.children.get(childIndex);
			int oldChildIndex = delete(currIndexNode, child, key, childIndex);
			if (oldChildIndex < 0) {
				return -1;
			} else {
				// delete the splitkey position in parent if the child is merged
				currIndexNode.keys.remove(oldChildIndex);
				currIndexNode.children.remove(oldChildIndex);
					
				// if index node is not underflowed
				if (!isUnderflow(currIndexNode)) {
					return -1;
				} else { // if the node is underflowed
					// Current node is the root node
					if (currIndexNode == this.root) {
						if (currIndexNode.children.size() == 1) {
							this.root = currIndexNode.children.get(0);
						}
						return 0;
					}
						
					// select the suitable sibling, and handle node underflow.
					IndexNode<K, T> sibling;
					oldChildIndex = -1; // init with no key to delete.
					if (currentIndex != 0) {
						sibling = (IndexNode<K, T>)parentNode.children.get(currentIndex - 1);
						oldChildIndex = handleIndexNodeUnderflow(sibling, currIndexNode, parentNode);
					} else {  // current node does not have left sibling
						sibling = (IndexNode<K, T>)parentNode.children.get(currentIndex + 1);
						oldChildIndex = handleIndexNodeUnderflow(currIndexNode, sibling, parentNode);
					}
					return oldChildIndex;
				}
			}
		} else { // current node is leaf node
			LeafNode<K, T> currLeafNode = (LeafNode<K, T>)currentNode;
			removeEntry(currLeafNode, key);
				
			// Check underflow
			if (!isUnderflow(currLeafNode)) {
				return -1;
			} else {
				if (currLeafNode == this.root) { // Current node is the root node
					if (currLeafNode.keys.size() == 0) {
						this.root = null;
					}
					return 0;
				}	
				// select the suitable sibling, and handle leaf node underflow
				LeafNode<K, T> sibling = currLeafNode.previousLeaf;
				int oldChildIndex = -1; // initially indicate no key to delete
				if (sibling != null && currentIndex != 0) {
					oldChildIndex = handleLeafNodeUnderflow(sibling, currLeafNode, parentNode);
				} else {  // current node does not have left sibling.
					sibling = currLeafNode.nextLeaf;
					oldChildIndex = handleLeafNodeUnderflow(currLeafNode, sibling, parentNode);
				}
				return oldChildIndex;
			}
		}
	}
		
	private void removeEntry(LeafNode<K, T> currLeafNode, K key) {
		int index = Collections.binarySearch(currLeafNode.keys, key);
		if (index < 0) {return;}
		currLeafNode.keys.remove(index);
		currLeafNode.values.remove(index);
	}
		
	private boolean isUnderflow(Node<K, T> node) {
		return (node.keys.size() < D);
	}

	/**
	 * TODO Handle LeafNode Underflow (merge or redistribution)
	 * 
	 * @param left
	 *            : the smaller node
	 * @param right
	 *            : the bigger node
	 * @param parent
	 *            : their parent index node
	 * @return the splitkey position in parent if merged so that parent can
	 *         delete the splitkey later on. -1 otherwise
	 */
	public int handleLeafNodeUnderflow(LeafNode<K,T> left, LeafNode<K,T> right, IndexNode<K,T> parent) {
		// The merge condition
		if (isLeafMergable(left, right)) {
			right.keys.addAll(0, left.keys);
			right.values.addAll(0, left.values);
					
			// Rearrange the next and previous pointer
			right.previousLeaf = left.previousLeaf;
			if (left.previousLeaf != null) {
				left.previousLeaf.nextLeaf = right;
			}
			return findKeyIndex(parent , right.keys.get(0));
		}	
		// The redistribution condition (cannot merge -> left.size + right.size > 2D)
		int leftSize = left.keys.size();
		if (leftSize < D) {  // left is the underflowed node
			/* locate the key index for the right node in the parent node.
			 * Note that the index for the key to find the right node
			 * is the child index of it's left sibling.
			 */
			int rightIndex = findKeyIndex(parent, left.keys.get(0));	
			left.keys.add(right.keys.get(0));
			left.values.add(right.values.get(0));
			right.keys.remove(0);
			right.values.remove(0);
			
			// change the key of right node in the parent node. 
			parent.keys.set(rightIndex, right.keys.get(0));			
		} else { //right is the underflowed node
			/* locate the key index for the right node in the parent node.
			 * Note that the index for the key to find the right node
			 * is the child index of it's left sibling.
			 */
			int rightIndex = findKeyIndex(parent , left.keys.get(0));
			right.keys.addAll(0, left.keys.subList(D, leftSize));
			right.values.addAll(0, left.values.subList(D, leftSize));
			left.keys.subList(D, leftSize).clear();
			left.values.subList(D, leftSize).clear();
					
			// change the key of right node in the parent node. 
			parent.keys.set(rightIndex, right.keys.get(0));
		}
		return -1;
	}
	
	private boolean isLeafMergable(Node<K,T> left, Node<K,T> right) {
		return (left.keys.size() + right.keys.size() <= D * 2);
	}

	/**
	 * TODO Handle IndexNode Underflow (merge or redistribution)
	 * 
	 * @param left
	 *            : the smaller node
	 * @param right
	 *            : the bigger node
	 * @param parent
	 *            : their parent index node
	 * @return the splitkey position in parent if merged so that parent can
	 *         delete the splitkey later on. -1 otherwise
	 */
	public int handleIndexNodeUnderflow(IndexNode<K,T> left, IndexNode<K,T> right, IndexNode<K,T> parent) {
		// The merge condition
		if (isIndexMergable(left, right)) {
			/* add the key from parent to the end of the left node, so that
			 * the index of the key equals the index of the last child. */ 
			int parentKeyIdx = findKeyIndex(parent , left.keys.get(0));
			left.keys.add(parent.keys.get(parentKeyIdx));
			right.keys.addAll(0, left.keys);
			right.children.addAll(0, left.children);
			return parentKeyIdx;
		}
				
		// The redistribution condition
		int leftKeySize = left.keys.size();
		int leftChildrenSize = left.children.size();		
		if (leftKeySize < D) { // if the left node is underflowed
			int parentKeyIndex = findKeyIndex(parent, left.keys.get(0));
			left.keys.add(parent.keys.get(parentKeyIndex));
			left.children.add(right.children.get(0));	
			// change the key in the parent node. 
			parent.keys.set(parentKeyIndex, right.keys.get(0));
			// remove the left most key-value pair in the right node.
			right.keys.remove(0);
			right.children.remove(0);
		} else { //right is the underflowed node
			/* locate the key index for the right node in the parent node.
			 * Note that the index for the key to find the right node
			 * is the child index of it's left sibling.
			 */
			int parentKeyIndex = findKeyIndex(parent, left.keys.get(0));	
			right.keys.add(0, parent.keys.get(parentKeyIndex));		
			right.keys.addAll(0, left.keys.subList(D + 1, leftKeySize));
			right.children.addAll(0, left.children.subList(D + 1, leftChildrenSize));
			left.keys.subList(D + 1, leftKeySize).clear();
			left.children.subList(D + 1, leftChildrenSize).clear();
			/* change the key of left node in the parent node, and remove it
			 * from the left node.
			 */
			parent.keys.set(parentKeyIndex, left.keys.get(D));
			left.keys.remove(D);		
		}
		return -1;
	}
	
	private boolean isIndexMergable(Node<K,T> left, Node<K,T> right) {
		return (left.keys.size() + 1 + right.keys.size() <= D * 2);
	}

}
