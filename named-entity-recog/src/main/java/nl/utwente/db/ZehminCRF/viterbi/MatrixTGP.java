package nl.utwente.db.ZehminCRF.viterbi;

import java.util.Vector;

import nl.utwente.db.ZehminCRF.corpus.Sentence;
import nl.utwente.db.ZehminCRF.sp.CRModel_sp1;

/**
 * @author Zhemin Zhu
 * Created on Aug 25, 2012
 *
 * CTIT Database Group
 * Universiteit Twente
 */
public class MatrixTGP{
	Vector<Vector<TagGainPre>> m_matrix;
	CRModel_sp1 m_model;
	Sentence m_s;
	
	//construct the matrix with promising tags for each word
	public MatrixTGP(Sentence s, CRModel_sp1 model){
		m_s = s;
		m_matrix = new Vector<Vector<TagGainPre>>();
		m_model = model;
		for(int i = 0; i < s.size(); ++ i){
			Vector<String> vecPromisingTags = m_model.getPromisingTags(s.getColumn(i));
			Vector<TagGainPre> vecTGP = new Vector<TagGainPre>();
			for(String tag : vecPromisingTags)
				vecTGP.add(new TagGainPre(tag, Double.NEGATIVE_INFINITY, -1));
			m_matrix.add(vecTGP);
		}
	}

	public void noPromisingTag(Sentence s, int column){
		Vector<TagGainPre> preVecTGP = getColumn(column - 1);
		Vector<TagGainPre> curVecTGP = getColumn(column);
		double maxValue = Double.NEGATIVE_INFINITY;
		int maxPre = -1;
		for(int i = 0; i < preVecTGP.size(); ++ i){
			if(preVecTGP.get(i).getGain() > maxValue){
				maxValue = preVecTGP.get(i).getGain();
				maxPre = i;
			}
		}
		for(TagGainPre tgp : curVecTGP){
			tgp.setPre(maxPre);
			tgp.setGain(m_model.getProb(tgp.getTag(), s.getColumn(column)));
		}
	}
	
	
	public Vector<TagGainPre> getColumn(int index){
		return m_matrix.get(index);
	}
	
	public Vector<TagGainPre> getFirstColumn(){
		return m_matrix.get(0);
	}
	
	public int getNumColum(){
		return m_matrix.size();
	}
	
	public int getNumRow(int index){
		return getColumn(index).size();
	}
	
	public Vector<TagGainPre> getLastColumn(){
		return getColumn(getNumColum() - 1);
	}
	
	
	public Vector<TagGainPre> getPath(TagGainPre lastTGP){
		Vector<TagGainPre> path = new Vector<TagGainPre>();
		assert lastTGP != null;
		path.add(0, lastTGP);
		int pre = lastTGP.getPre();
		int colIndex = getNumColum() - 1;
		while(pre != -1){
			TagGainPre tgp = getColumn(-- colIndex).get(pre);
			path.add(0, tgp);
			pre = tgp.m_pre;
		}
		assert path.size() == getNumColum();
		return path;
	}
	
	public Vector<String> findMaxPath(){
		Vector<TagGainPre> lastVecTGP = getLastColumn();
		Vector<String> maxPathString = new Vector<String>();
		double max = Double.NEGATIVE_INFINITY;
		TagGainPre maxLastTGP = null;
		for(TagGainPre tgp : lastVecTGP){
			if(tgp.getGain() > max){
				max = tgp.getGain();
				maxLastTGP = tgp;
			}
		}
		Vector<TagGainPre> maxPathTGP = getPath(maxLastTGP);
		for(TagGainPre tgp : maxPathTGP)
			maxPathString.add(tgp.getTag());
		return maxPathString;
	}
	
	public void printTGP(int column, String tag){
		Vector<TagGainPre> vecTGP = getColumn(column);
		for(TagGainPre tgp : vecTGP){
			if(tgp.getTag().equals(tag)){
				tgp.print();
				return;
			}
		}
	}
	
	public void printMatrix(){
		for(int i = 0; i < getNumColum(); ++ i){
			printVecTGP(i);
		}
	}
	
	public void printVecTGP(int column){
		System.out.println(m_s.getWord(column));
		Vector<TagGainPre> vecTGP = getColumn(column);
		for(int i = 0; i < vecTGP.size(); ++ i){
			if(vecTGP.get(i).getGain() <= Double.NEGATIVE_INFINITY)
				continue;
			System.out.print(i + ": ");
			vecTGP.get(i).print();
		}
		System.out.println("");
	}
	
	public void printPath(Vector<TagGainPre> path){
		for(int i = 0; i < path.size(); ++ i){
			TagGainPre tgp = path.get(i);
			String word = m_s.getWord(i);
			System.out.println(word + ": " + tgp.toString());
		}
	}
	
	
	public Vector<Vector<TagGainPre>> getPath(int column, String tag){
		Vector<Vector<TagGainPre>> paths = new Vector<Vector<TagGainPre>>();
		Vector<TagGainPre> vecLastTGP = getLastColumn();
		for(int i = 0; i < vecLastTGP.size(); ++ i){
			TagGainPre lastTgp = vecLastTGP.get(i);
			Vector<TagGainPre> path = getPath(lastTgp);
			if(path.get(column).getTag().equals(tag)){
				paths.add(path);
				System.out.println("\n\n Path " + i);
				printPath(path);
			}
		}
		return paths;
	}
	
	
	//topK paths
	/*public List<Path> topKPath(int k){
		Vector<Path> AllPaths = listAllPaths();
		if(k > AllPaths.size()){
			System.err.println("There is no enough promising paths!");
			System.err.println("Only top" + AllPaths.size() + " paths returned!");
			return AllPaths; 
		}else{
			return AllPaths.subList(0, k);
		}
	}
	
	
	
	private Vector<Path> listAllPaths(){
		int numAllPaths = 1;
		Vector<Integer> branchNums = new Vector<Integer>();
		for(int i = getNumColum() - 1; i >= 0; -- i){
			branchNums.add(0, numAllPaths);
			numAllPaths *= getNumRow(i);
		}
		System.out.println("#All promising paths:\t" + numAllPaths);
		Vector<Path> paths = new Vector<Path>();
		for(int i = 0; i < numAllPaths; ++i)
			paths.add(new Path());
		
		for(int i = 0; i < getNumColum(); ++ i){
			int branchNum = branchNums.get(i);
			Vector<TagGainPre> vecColumn = getColumn(i);
			for(int k = 0; k < vecColumn.size(); ++ k){
				TagGainPre tgp = vecColumn.get(k);
				String tag = tgp.getTag();
				for(int j = 0; j < branchNum; ++ j)
					paths.get(branchNum * k + j).addTag(tag);
			}
		}
		
		for(Path path : paths)
			score(path, m_s);
		Collections.sort(paths, new Path.PathComparable());
		return paths;
	}
	
	// assign score to a path
	private double score(Path path, Sentence s){
		CRStepGain stepGain = new CRStepGain(m_model);
		double score = 0.0;
		for(int i = 0; i < path.getSize(); ++ i){
			String curTag = path.getTag(i);
			score += m_model.getProb(curTag, s.getColumn(i));
		}
		for(int i = 0; i < path.getSize() - 1; ++ i){
			String curTag = path.getTag(i);
			String nextTag = path.getTag(i + 1);
			score += m_model.getCR(curTag, nextTag, s.getColumn(i), s.getColumn(i + 1));
		}
		path.setScore(score);
		return score;
	}*/
	
	
}
