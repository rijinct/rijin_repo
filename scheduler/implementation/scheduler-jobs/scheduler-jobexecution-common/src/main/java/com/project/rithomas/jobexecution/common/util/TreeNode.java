package com.project.rithomas.jobexecution.common.util;

import java.util.ArrayList;
import java.util.List;

public class TreeNode<T> {
	private T data;
	private List<TreeNode<T>> parents = new ArrayList<>();
	private List<TreeNode<T>> children = new ArrayList<>();
	
	public TreeNode(T data) {
		this.data = data;
	}
	
	public void addChild(TreeNode<T> child){
		this.children.add(child);
		child.parents.add(this);
	}

	public List<TreeNode<T>> getLeafNodes() {
		List<TreeNode<T>> leafNodes = new ArrayList<>();
		for (TreeNode<T> child : getChildren()) {
			if (child.getChildren().isEmpty()) {
				leafNodes.add(child);
			} else {
				leafNodes.addAll(child.getLeafNodes());
			}
		}
		return leafNodes;
	}

	public T getData() {
		return data;
	}

	public void setData(T data) {
		this.data = data;
	}

	public List<TreeNode<T>> getParents() {
		return new ArrayList<>(parents);
	}

	public List<TreeNode<T>> getChildren() {
		return new ArrayList<>(children);
	}
	
	@Override
	public String toString() {
		StringBuilder output = new StringBuilder(data.toString())
				.append(", Children: [");
		for (TreeNode<T> child : children) {
			output = output.append(child.getData().toString()).append("\n");
		}
		output = output.append("]");
		return output.toString();
	}
}
