package nl.utwente.db.ZehminCRF.viterbi;

import java.util.Comparator;
import java.util.Vector;

/**
 * @author Zhemin Zhu
 * Created on Apr 8, 2013
 *
 * CTIT Database Group
 * Universiteit Twente
 */
public class Path {
	Vector<String> m_tags;
	double m_score;
	
	public boolean isEqual(Path path){
		Vector<String> tags = path.getTags();
		if(tags.size() != m_tags.size())
			return false;
		for(int i = 0; i < tags.size(); ++ i)
			if(! tags.get(i).equals(m_tags.get(i)))
				return false;
		return true;
	}
	
	public String getPath(){
		StringBuilder sb = new StringBuilder();
		for(String tag : m_tags)
			sb.append(tag + " ");
		return sb.toString();
	}
	
	public Path(String strTags){
		m_tags = new Vector<String>();
		String[] tags = strTags.split(" ");
		for(int i = 0; i < tags.length; ++ i)
			m_tags.add(tags[i]);
	}
	
	public Path(Vector<String> tags){
		m_tags = tags;
	}
	
	public Vector<String> getTags(){
		return m_tags;
	}
	
	public Path(){
		m_tags = new Vector<String>();
	}
	
	public double getScore(){
		return m_score;
	}
	
	public String getTag(int i){
		return m_tags.get(i);
	}

	public void setScore(double score){
		m_score = score;
	}
	
	public void addTag(String tag){
		m_tags.add(tag);
	}
	
	public int getSize(){
		return m_tags.size();
	}
	
	public String toString(){
		StringBuilder sb = new StringBuilder();
		for(String tag : m_tags){
			sb.append(tag + " ");
		}
		sb.append("\n Score:\t" + m_score);
		return sb.toString();
	}
	
	
	public void print(){
		System.out.print(toString());
	}
	
	public static class PathComparable implements Comparator<Path>{
	    public int compare(Path path1, Path path2) {
	    	return path1.getScore() > path2.getScore() ? 0 : 1;
	    }
	}
	
}
