package nl.utwente.db.ZehminCRF.sp;

import java.util.HashMap;
import java.util.Vector;

import nl.utwente.db.ZehminCRF.corpus.Corpus;
import nl.utwente.db.ZehminCRF.utils.MyTimer;

/**
 * @author Zhemin Zhu Created on Oct 12, 2012
 *
 * CTIT Database Group Universiteit Twente
 */
//public class CRModel_sp1
//{
//
//    Corpus m_corpus;
//    HashMap<String, CPT> m_cpto;
//    HashMap<String, CPT> m_cptf;
//    HashMap<String, CPT> m_cptoo;
//    HashMap<String, CPT> m_cptff;
//    HashMap<String, CPT> m_cptff1;
//    HashMap<String, CPT> m_cptff2;
//    HashMap<String, CPT> m_cptfo;
//    HashMap<String, CPT> m_cptof;
//    HashMap<String, Double> m_crss;
//    HashMap<String, Double> m_s;
//    Vector<String> m_tagSpace;
//
//    //bTraining = true: training  bTraining = false : decoding
//    //corpus: training corpus
//    public CRModel_sp1(Corpus corpus, String modelFile, boolean bTraining)
//    {
//        if (bTraining)
//        {//training
//            m_corpus = corpus;
//        }
//        else
//        { //decoding
//            m_corpus = corpus;
//            initCPT();
//            System.out.println("Init CPTs Finished! Start to decode...");
//        }
//    }
//
//    private void initCPT()
//    {
//        m_cpto = m_corpus.constructCPTO();
//        m_cptf = m_corpus.constructCPTF();
//        m_cptoo = m_corpus.constructCPTOO();
//        m_cptff = m_corpus.constructCPTFF();
//        m_cptff1 = m_corpus.constructCPTFF1();
//        m_cptff2 = m_corpus.constructCPTFF2();
//        m_cptfo = m_corpus.constructCPTFO();
//        m_cptof = m_corpus.constructCPTOF();
//        m_crss = m_corpus.constructEmpCRSS();
//        m_s = m_corpus.constructEmpProbS();
//        m_tagSpace = m_corpus.getTagSpace();
//    }
//
//    public double getCR(String s1, String s2, Vector<String> vf1, Vector<String> vf2)
//    {
//        String OO = vf1.firstElement() + " " + vf2.firstElement();
//        String FF = vf1.get(1) + " " + vf2.get(1);
//        if (m_cptoo.containsKey(OO))
//        {
//            //non-OOV
//            double probOO = m_cptoo.get(OO).getProb(s1 + " " + s2);
//            if (probOO <= Double.NEGATIVE_INFINITY)
//            {
//                return - 100000;
//            }
//            double oocr = probOO
//                    - getProb(s1, vf1)
//                    - getProb(s2, vf2);
//            return oocr;
//        }
//        else
//        {
//            //OOV
//            if (!m_cptff.containsKey(FF))
//            {
//                //System.err.println("OOFF: " + FF);
//                Double crss = m_corpus.constructEmpCRSS().get(s1 + " " + s2);
//                if (crss == null)
//                {
//                    return - 100000;
//                }
//                return crss;
//            }
//
//            double probFF = m_cptff.get(FF).getProb(s1 + " " + s2);
//            if (probFF <= Double.NEGATIVE_INFINITY)
//            {
//                return - 100000;
//            }
//
//            double ffcr = probFF
//                    - m_cptff1.get(FF + "1").getProb(s1)
//                    - m_cptff2.get(FF + "2").getProb(s2);
//
//            return 0.65 * ffcr;   //to be adjusted using development dataset
//            //return  ffcr;
//        }
//    }
//
//    public double getProb(String s, Vector<String> vf)
//    {
//        String O = vf.firstElement();
//        String F = vf.get(1);
//        if (m_cpto.containsKey(O))
//        {
//            //non-OOV
//            double probO = m_cpto.get(O).getProb(s);
//            if (probO <= Double.NEGATIVE_INFINITY)
//            {
//                return - 100000;
//            }
//            return probO;
//        }
//        else
//        {
//            if (!m_cptf.containsKey(F))
//            {
//                //System.err.println("OOF: " + F);
//                Double ps = m_s.get(s);
//                if (ps == null)
//                {
//                    return - 100000;
//                }
//                return ps;
//            }
//            //OOV
//            //to be adjusted using development dataset
//            return 0.95 * m_cptf.get(F).getProb(s);
//            //return m_CPT.get(F).getProb(s);
//        }
//    }
//
//    public Vector<String> getPromisingTags(Vector<String> vf)
//    {
//        if (m_cpto.containsKey(vf.firstElement()))
//        {//non-OOV
//            return m_cpto.get(vf.firstElement()).getTagSpace();
//        }
//        else
//        {//OOV
//            if (!m_cptf.containsKey(vf.get(1)))
//            {
//                //OOF
//                return m_tagSpace;
//            }
//            return m_cptf.get(vf.get(1)).getTagSpace();
//        }
//    }
//
//    /*
//     *
//     * TopK paths
//     *
//     */
//    public Vector<Path> getPromisingPaths(Sentence s)
//    {
//        Vector<String> paths = getPairwisePromisingTags(s.getColumn(0), s.getColumn(1));
//        for (int i = 1; i < s.size() - 1; ++i)
//        {
//            Vector<String> vf1 = s.getColumn(i);
//            Vector<String> vf2 = s.getColumn(i + 1);
//            Vector<String> pairwiseTags = getPairwisePromisingTags(vf1, vf2);
//            paths = pathJoin(paths, pairwiseTags, s.getColumn(i));
//        }
//
//        Vector<Path> results = new Vector<Path>();
//        for (String strPath : paths)
//        {
//            Path path = new Path(strPath);
//            score(path, s);
//            results.add(path);
//        }
//        Collections.sort(results, new Path.PathComparable());
//        return results;
//    }
//
//    public double score(Path path, Sentence s)
//    {
//        CRStepGain stepGain = new CRStepGain(this);
//        double score = 0.0;
//        for (int i = 0; i < path.getSize(); ++i)
//        {
//            String curTag = path.getTag(i);
//            score += getProb(curTag, s.getColumn(i));
//        }
//        for (int i = 0; i < path.getSize() - 1; ++i)
//        {
//            String curTag = path.getTag(i);
//            String nextTag = path.getTag(i + 1);
//            score += getCR(curTag, nextTag, s.getColumn(i), s.getColumn(i + 1));
//        }
//        path.setScore(score);
//        return score;
//    }
//
//    private Vector<String> getPairwisePromisingTags(Vector<String> vf1, Vector<String> vf2)
//    {
//        Vector<String> pairwisePaths = new Vector<String>();
//        String w1 = vf1.firstElement();
//        String f1 = vf1.get(1);
//        String w2 = vf2.firstElement();
//        String f2 = vf2.get(1);
//        CPT cptoo = m_cptoo.get(w1 + " " + w2);
//        if (cptoo != null)
//        {
//            pairwisePaths.addAll(cptoo.getTagSpace());
//            return pairwisePaths;
//        }
//        else
//        {
//            CPT cpt1 = m_cpto.get(w1);
//            CPT cpt2 = m_cpto.get(w2);
//            Vector<String> ts1 = null;
//            Vector<String> ts2 = null;
//            if (cpt1 == null && cpt2 == null)
//            { //both are oov
//                CPT cpt = m_cptff.get(f1 + " " + f2);
//                if (cpt != null)
//                {
//                    return cpt.getTagSpace();
//                }
//
//                CPT cptf = m_cptf.get(f1);
//                if (cptf != null)
//                {
//                    ts1 = cptf.getTagSpace();
//                }
//                else
//                {
//                    ts1 = m_tagSpace;
//                }
//
//                cptf = m_cptf.get(f2);
//                if (cptf != null)
//                {
//                    ts2 = cptf.getTagSpace();
//                }
//                else
//                {
//                    ts2 = m_tagSpace;
//                }
//
//            }
//            else if (cpt1 == null)
//            {//w1 is oov
//                CPT cpt = m_cptfo.get(f1 + " " + w2);
//                if (cpt != null)
//                {
//                    return cpt.getTagSpace();
//                }
//                cpt = m_cptff.get(f1 + " " + f2);
//                if (cpt != null)
//                {
//                    Vector<String> result = new Vector<String>();
//                    Vector<String> tt = cpt.getTagSpace();
//                    Vector<String> t2 = m_cpto.get(w2).getTagSpace();
//                    HashMap<String, Boolean> t2map = new HashMap<String, Boolean>();
//                    for (String tag2 : t2)
//                    {
//                        t2map.put(tag2, true);
//                    }
//                    for (String tagtag : tt)
//                    {
//                        if (t2map.containsKey(tagtag.split(" ")[1]))
//                        {
//                            result.add(tagtag);
//                        }
//                    }
//                    return result;
//                }
//                CPT cptf = m_cptf.get(f1);
//                if (cptf != null)
//                {
//                    ts1 = cptf.getTagSpace();
//                }
//                else
//                {
//                    ts1 = m_tagSpace;
//                }
//                ts2 = m_cpto.get(w2).getTagSpace();
//            }
//            else if (cpt2 == null)
//            {//w2 is oov
//                CPT cpt = m_cptof.get(w1 + " " + f2);
//                if (cpt != null)
//                {
//                    return cpt.getTagSpace();
//                }
//                cpt = m_cptff.get(f1 + " " + f2);
//                if (cpt != null)
//                {
//                    Vector<String> result = new Vector<String>();
//                    Vector<String> tt = cpt.getTagSpace();
//                    Vector<String> t1 = m_cpto.get(w1).getTagSpace();
//                    HashMap<String, Boolean> t1map = new HashMap<String, Boolean>();
//                    for (String tag1 : t1)
//                    {
//                        t1map.put(tag1, true);
//                    }
//                    for (String tagtag : tt)
//                    {
//                        if (t1map.containsKey(tagtag.split(" ")[0]))
//                        {
//                            result.add(tagtag);
//                        }
//                    }
//                    return result;
//                }
//                ts1 = m_cpto.get(w1).getTagSpace();
//                CPT cptf = m_cptf.get(f2);
//                if (cptf != null)
//                {
//                    ts2 = cptf.getTagSpace();
//                }
//                else
//                {
//                    ts2 = m_tagSpace;
//                }
//            }
//            else
//            {
//                ts1 = cpt1.getTagSpace();
//                ts2 = cpt2.getTagSpace();
//            }
//
//            Vector<String> product = CartesianProduct(ts1, ts2);
//            for (String ss : product)
//            {
//                if (m_crss.containsKey(ss))
//                {
//                    pairwisePaths.add(ss);
//                }
//            }
//
//            if (pairwisePaths.size() == 0)
//            {
//                for (String ss : product)
//                {
//                    pairwisePaths.add(ss);
//                }
//            }
//
//            return pairwisePaths;
//        }
//    }
//    
////    private Vector<String> getPairwisePromisingTags(Vector<String> vf1, Vector<String> vf2)
////    {
////        Vector<String> pairwisePaths = new Vector<String>();
////        String w1 = vf1.firstElement();
////        String f1 = vf1.get(1);
////        String w2 = vf2.firstElement();
////        String f2 = vf2.get(1);
////        CPT cptoo = m_cptoo.get(w1 + " " + w2);
////        if (cptoo != null)
////        {
////            pairwisePaths.addAll(cptoo.getTagSpace());
////            return pairwisePaths;
////        }
////        else
////        {
////            CPT cpt1 = m_cpto.get(w1);
////            CPT cpt2 = m_cpto.get(w2);
////            Vector<String> ts1 = null;
////            Vector<String> ts2 = null;
////            if (cpt1 == null && cpt2 == null)
////            { //both are oov
////                CPT cpt = m_cptff.get(f1 + " " + f2);
////                if (cpt != null)
////                {
////                    return cpt.getTagSpace();
////                }
////                ts1 = m_cptf.get(f1).getTagSpace();
////                ts2 = m_cptf.get(f2).getTagSpace();
////            }
////            else if (cpt1 == null)
////            {//w1 is oov
////                CPT cpt = m_cptfo.get(f1 + " " + w2);
////                if (cpt != null)
////                {
////                    return cpt.getTagSpace();
////                }
////                cpt = m_cptff.get(f1 + " " + f2);
////                if (cpt != null)
////                {
////                    Vector<String> result = new Vector<String>();
////                    Vector<String> tt = cpt.getTagSpace();
////                    Vector<String> t2 = m_cpto.get(w2).getTagSpace();
////                    HashMap<String, Boolean> t2map = new HashMap<String, Boolean>();
////                    for (String tag2 : t2)
////                    {
////                        t2map.put(tag2, true);
////                    }
////                    for (String tagtag : tt)
////                    {
////                        if (t2map.containsKey(tagtag.split(" ")[1]))
////                        {
////                            result.add(tagtag);
////                        }
////                    }
////                    return result;
////                }
////                ts1 = m_cptf.get(f1).getTagSpace();
////                ts2 = m_cpto.get(w2).getTagSpace();
////            }
////            else if (cpt2 == null)
////            {//w2 is oov
////                CPT cpt = m_cptof.get(w1 + " " + f2);
////                if (cpt != null)
////                {
////                    return cpt.getTagSpace();
////                }
////                cpt = m_cptff.get(f1 + " " + f2);
////                if (cpt != null)
////                {
////                    Vector<String> result = new Vector<String>();
////                    Vector<String> tt = cpt.getTagSpace();
////                    Vector<String> t1 = m_cpto.get(w1).getTagSpace();
////                    HashMap<String, Boolean> t1map = new HashMap<String, Boolean>();
////                    for (String tag1 : t1)
////                    {
////                        t1map.put(tag1, true);
////                    }
////                    for (String tagtag : tt)
////                    {
////                        if (t1map.containsKey(tagtag.split(" ")[0]))
////                        {
////                            result.add(tagtag);
////                        }
////                    }
////                    return result;
////                }
////                ts1 = m_cpto.get(w1).getTagSpace();
////                ts2 = m_cptf.get(f2).getTagSpace();
////            }
////            else
////            {
////                ts1 = cpt1.getTagSpace();
////                ts2 = cpt2.getTagSpace();
////            }
////
////            Vector<String> product = CartesianProduct(ts1, ts2);
////            for (String ss : product)
////            {
////                if (m_crss.containsKey(ss))
////                {
////                    pairwisePaths.add(ss);
////                }
////            }
////
////            if (pairwisePaths.size() == 0)
////            {
////                for (String ss : product)
////                {
////                    pairwisePaths.add(ss);
////                }
////            }
////
////            return pairwisePaths;
////        }
////    }
//
//
//    private Vector<String> CartesianProduct(Vector<String> ps1, Vector<String> ps2)
//    {
//        Vector<String> results = new Vector<String>();
//        for (String p1 : ps1)
//        {
//            for (String p2 : ps2)
//            {
//                results.add(p1 + " " + p2);
//            }
//        }
//        return results;
//    }
//
//    private Vector<String> pathJoin(Vector<String> paths1, Vector<String> paths2, Vector<String> vf)
//    {
//
//        Vector<String> paths = new Vector<String>();
//        HashMap<String, Boolean> intersects = new HashMap<String, Boolean>();
//        for (String path1 : paths1)
//        {
//            String[] tags1 = path1.split(" ");
//            String last1 = tags1[tags1.length - 1];
//            intersects.put(last1, true);
//            for (String path2 : paths2)
//            {
//                String first2 = path2.split(" ")[0];
//                intersects.put(first2, true);
//                if (last1.equals(first2))
//                {
//                    paths.add(path1 + " " + path2.substring(first2.length()).trim());
//                }
//            }
//        }
//
//        if (paths.size() != 0 && paths.size() * 1.0 / paths1.size() < 5)
//        {
//            return paths;
//        }
//
//
//        paths.clear();
//
//        Set<String> tags = intersects.keySet();
//        double maxProb = Double.NEGATIVE_INFINITY;
//        String maxTag = null;
//        for (String tag : tags)
//        {
//            if (maxTag == null)
//            {
//                maxTag = tag;
//            }
//            double curProb = getProb(tag, vf);
//            if (curProb > maxProb)
//            {
//                maxTag = tag;
//                maxProb = curProb;
//            }
//        }
//
//        HashMap<String, Boolean> subpaths1 = new HashMap<String, Boolean>();
//        HashMap<String, Boolean> subpaths2 = new HashMap<String, Boolean>();
//
//
//        for (String path1 : paths1)
//        {
//            String[] tags1 = path1.split(" ");
//            String last1 = tags1[tags1.length - 1];
//            String subpath1 = path1.substring(0, path1.length() - last1.length()).trim();
//            subpaths1.put(subpath1, true);
//        }
//
//        for (String path2 : paths2)
//        {
//            String first2 = path2.split(" ")[0];
//            String subpath2 = path2.substring(first2.length()).trim();
//            subpaths2.put(subpath2, true);
//        }
//
//
//        Set<String> subpaths1_unique = subpaths1.keySet();
//        Set<String> subpaths2_unique = subpaths2.keySet();
//
//        for (String subpath1 : subpaths1_unique)
//        {
//            for (String subpath2 : subpaths2_unique)
//            {
//                paths.add(subpath1 + " " + maxTag + " " + subpath2);
//            }
//        }
//
//
//        if (paths.size() * 1.0 / paths1.size() > 5)
//        {
//            double maxScore = Double.NEGATIVE_INFINITY;
//            String maxPath2 = null;
//            for (String subpath2 : subpaths2_unique)
//            {
//                Double curScore = m_crss.get(maxTag + " " + subpath2);
//                if (curScore != null && curScore > maxScore)
//                {
//                    maxScore = curScore;
//                    maxPath2 = subpath2;
//                }
//            }
//
//            paths.clear();
//            for (String subpath1 : subpaths1_unique)
//            {
//                paths.add(subpath1 + " " + maxTag + " " + maxPath2);
//            }
//            return paths;
//        }
//        return paths;
//    }
//    /*
//     * public void train(){ MyTimer timer = new MyTimer(); timer.start();
//     * initCPT(); m_corpus.printStatistics(); timer.end(); timer.printTime();
//     * System.out.println("Train Finished!");
//	}
//     */
//}
public class CRModel_sp1
{

    Corpus m_corpus;
    HashMap<String, CPT> m_cpto;
    HashMap<String, CPT> m_cptf;
    HashMap<String, CPT> m_cptoo;
    HashMap<String, CPT> m_cptff;
    HashMap<String, CPT> m_cptff1;
    HashMap<String, CPT> m_cptff2;
    HashMap<String, CPT> m_cptfo;
    HashMap<String, CPT> m_cptof;
    HashMap<String, Double> m_crss;
    HashMap<String, Double> m_s;
    Vector<String> m_tagSpace;

    //bTraining = true: training  bTraining = false : decoding
    //corpus: training corpus
    public CRModel_sp1(Corpus corpus, String modelFile, boolean bTraining)
    {
        if (bTraining)
        {//training
            m_corpus = corpus;
        }
        else
        { //decoding
            m_corpus = corpus;
            initCPT();
            System.out.println("Init CPTs Finished! Start to decode...");
        }
    }

    private void initCPT()
    {
        m_cpto = m_corpus.constructCPTO();
        m_cptf = m_corpus.constructCPTF();
        m_cptoo = m_corpus.constructCPTOO();
        m_cptff = m_corpus.constructCPTFF();
        m_cptff1 = m_corpus.constructCPTFF1();
        m_cptff2 = m_corpus.constructCPTFF2();
       // m_cptfo = m_corpus.constructCPTFO();
        //m_cptof = m_corpus.constructCPTOF();
        m_crss = m_corpus.constructEmpCRSS();
        m_s = m_corpus.constructEmpProbS();
        m_tagSpace = m_corpus.getTagSpace();
    }

    public double getCR(String s1, String s2, Vector<String> vf1, Vector<String> vf2)
    {
        String OO = vf1.firstElement() + " " + vf2.firstElement();
        String FF = vf1.get(1) + " " + vf2.get(1);
        if (m_cptoo.containsKey(OO))
        {
            //non-OOV
            double probOO = m_cptoo.get(OO).getProb(s1 + " " + s2);
            if (probOO <= Double.NEGATIVE_INFINITY)
            {
                return - 100000;
            }
            double oocr = probOO
                    - getProb(s1, vf1)
                    - getProb(s2, vf2);
            return oocr;
        }
        else
        {
            //OOV
            if (!m_cptff.containsKey(FF))
            {
                //System.err.println("OOFF: " + FF);
                Double crss = m_corpus.constructEmpCRSS().get(s1 + " " + s2);
                if (crss == null)
                {
                    return - 100000;
                }
                return crss;
            }

            double probFF = m_cptff.get(FF).getProb(s1 + " " + s2);
            if (probFF <= Double.NEGATIVE_INFINITY)
            {
                return - 100000;
            }

            double ffcr = probFF
                    - m_cptff1.get(FF + "1").getProb(s1)
                    - m_cptff2.get(FF + "2").getProb(s2);
            return 0.65 * ffcr;   //to be adjusted using development dataset
            //return  ffcr;
        }
    }

    public double getProb(String s, Vector<String> vf)
    {
        String O = vf.firstElement();
        String F = vf.get(1);
        if (m_cpto.containsKey(O))
        {
            //non-OOV
            double probO = m_cpto.get(O).getProb(s);
            if (probO <= Double.NEGATIVE_INFINITY)
            {
                return - 100000;
            }
            return probO;
        }
        else
        {
            if (!m_cptf.containsKey(F))
            {
                //System.err.println("OOF: " + F);
                Double ps = m_s.get(s);
                if (ps == null)
                {
                    return - 100000;
                }
                return ps;
            }
            //OOV
            //to be adjusted using development dataset
            //0.95 POS
            return 0.95 * m_cptf.get(F).getProb(s);
            //return m_CPT.get(F).getProb(s);
        }
    }

    public Vector<String> getPromisingTags(Vector<String> vf)
    {
        if (m_cpto.containsKey(vf.firstElement()))
        {//non-OOV
            return m_cpto.get(vf.firstElement()).getTagSpace();
        }
        else
        {//OOV
            if (!m_cptf.containsKey(vf.get(1)))
            {
                //OOF
                return m_tagSpace;
            }
            return m_cptf.get(vf.get(1)).getTagSpace();
        }
    }

    /*
     *
     * TopK paths
     *
     */
//    public Vector<Path> getPromisingPaths(Sentence s)
//    {
//        Vector<String> paths = getPairwisePromisingTags(s.getColumn(0), s.getColumn(1));
//        for (int i = 1; i < s.size() - 1; ++i)
//        {
//            Vector<String> vf1 = s.getColumn(i);
//            Vector<String> vf2 = s.getColumn(i + 1);
//            Vector<String> pairwiseTags = getPairwisePromisingTags(vf1, vf2);
//            paths = pathJoin(paths, pairwiseTags, s.getColumn(i));
//        }
//
//        /*
//         * if(s.getWord(1).equals("AMBER") && s.getWord(2).equals("ALERT")){
//         * System.out.println("Same Path!!!"); for(String path : paths){
//         * System.out.println(path); } }
//         *
//         * if(isSamePath(paths)) System.out.println("Same Path!!!");
//         */
//
//        Vector<Path> results = new Vector<Path>();
//        for (String strPath : paths)
//        {
//            Path path = new Path(strPath);
//            score(path, s);
//            results.add(path);
//        }
//        Collections.sort(results, new Path.PathComparable());
//        return results;
//    }

//    public double score(Path path, Sentence s)
//    {
//        CRStepGain stepGain = new CRStepGain(this);
//        double score = 0.0;
//        for (int i = 0; i < path.getSize(); ++i)
//        {
//            String curTag = path.getTag(i);
//            score += getProb(curTag, s.getColumn(i));
//        }
//        for (int i = 0; i < path.getSize() - 1; ++i)
//        {
//            String curTag = path.getTag(i);
//            String nextTag = path.getTag(i + 1);
//            score += getCR(curTag, nextTag, s.getColumn(i), s.getColumn(i + 1));
//        }
//        path.setScore(score);
//        return score;
//    }

//    private Vector<String> getPairwisePromisingTags(Vector<String> vf1, Vector<String> vf2)
//    {
//        Vector<String> pairwisePaths = new Vector<String>();
//        String w1 = vf1.firstElement();
//        String f1 = vf1.get(1);
//        String w2 = vf2.firstElement();
//        String f2 = vf2.get(1);
//        CPT cptoo = m_cptoo.get(w1 + " " + w2);
//        if (cptoo != null)
//        {
//            pairwisePaths.addAll(cptoo.getTagSpace());
//            return pairwisePaths;
//        }
//        else
//        {
//            CPT cpt1 = m_cpto.get(w1);
//            CPT cpt2 = m_cpto.get(w2);
//            Vector<String> ts1 = null;
//            Vector<String> ts2 = null;
//            if (cpt1 == null && cpt2 == null)
//            { //both are oov
//                CPT cpt = m_cptff.get(f1 + " " + f2);
//                if (cpt != null)
//                {
//                    return cpt.getTagSpace();
//                }
//
//                CPT cptf = m_cptf.get(f1);
//                if (cptf != null)
//                {
//                    ts1 = cptf.getTagSpace();
//                }
//                else
//                {
//                    ts1 = m_tagSpace;
//                }
//
//                cptf = m_cptf.get(f2);
//                if (cptf != null)
//                {
//                    ts2 = cptf.getTagSpace();
//                }
//                else
//                {
//                    ts2 = m_tagSpace;
//                }
//
//            }
//            else if (cpt1 == null)
//            {//w1 is oov
//                CPT cpt = m_cptfo.get(f1 + " " + w2);
//                if (cpt != null)
//                {
//                    return cpt.getTagSpace();
//                }
//                cpt = m_cptff.get(f1 + " " + f2);
//                if (cpt != null)
//                {
//                    Vector<String> result = new Vector<String>();
//                    Vector<String> tt = cpt.getTagSpace();
//                    Vector<String> t2 = m_cpto.get(w2).getTagSpace();
//                    HashMap<String, Boolean> t2map = new HashMap<String, Boolean>();
//                    for (String tag2 : t2)
//                    {
//                        t2map.put(tag2, true);
//                    }
//                    for (String tagtag : tt)
//                    {
//                        if (t2map.containsKey(tagtag.split(" ")[1]))
//                        {
//                            result.add(tagtag);
//                        }
//                    }
//                    return result;
//                }
//                CPT cptf = m_cptf.get(f1);
//                if (cptf != null)
//                {
//                    ts1 = cptf.getTagSpace();
//                }
//                else
//                {
//                    ts1 = m_tagSpace;
//                }
//                ts2 = m_cpto.get(w2).getTagSpace();
//            }
//            else if (cpt2 == null)
//            {//w2 is oov
//                CPT cpt = m_cptof.get(w1 + " " + f2);
//                if (cpt != null)
//                {
//                    return cpt.getTagSpace();
//                }
//                cpt = m_cptff.get(f1 + " " + f2);
//                if (cpt != null)
//                {
//                    Vector<String> result = new Vector<String>();
//                    Vector<String> tt = cpt.getTagSpace();
//                    Vector<String> t1 = m_cpto.get(w1).getTagSpace();
//                    HashMap<String, Boolean> t1map = new HashMap<String, Boolean>();
//                    for (String tag1 : t1)
//                    {
//                        t1map.put(tag1, true);
//                    }
//                    for (String tagtag : tt)
//                    {
//                        if (t1map.containsKey(tagtag.split(" ")[0]))
//                        {
//                            result.add(tagtag);
//                        }
//                    }
//                    return result;
//                }
//                ts1 = m_cpto.get(w1).getTagSpace();
//                CPT cptf = m_cptf.get(f2);
//                if (cptf != null)
//                {
//                    ts2 = cptf.getTagSpace();
//                }
//                else
//                {
//                    ts2 = m_tagSpace;
//                }
//            }
//            else
//            {
//                ts1 = cpt1.getTagSpace();
//                ts2 = cpt2.getTagSpace();
//            }
//
//            Vector<String> product = CartesianProduct(ts1, ts2);
//            for (String ss : product)
//            {
//                if (m_crss.containsKey(ss))
//                {
//                    pairwisePaths.add(ss);
//                }
//            }
//
//            if (pairwisePaths.size() == 0)
//            {
//                for (String ss : product)
//                {
//                    pairwisePaths.add(ss);
//                }
//            }
//
//            return pairwisePaths;
//        }
//    }

//    private Vector<String> CartesianProduct(Vector<String> ps1, Vector<String> ps2)
//    {
//        Vector<String> results = new Vector<String>();
//        for (String p1 : ps1)
//        {
//            for (String p2 : ps2)
//            {
//                results.add(p1 + " " + p2);
//            }
//        }
//        return results;
//    }

//    private boolean isSamePath(Vector<String> paths)
//    {
//        HashMap<String, Boolean> map = new HashMap<String, Boolean>();
//        for (String path : paths)
//        {
//            if (map.containsKey(path.trim()))
//            {
//                return true;
//            }
//            else
//            {
//                map.put(path.trim(), true);
//            }
//        }
//        return false;
//    }

//    private Vector<String> pathJoin(Vector<String> paths1, Vector<String> paths2, Vector<String> vf)
//    {
//
//        Vector<String> paths = new Vector<String>();
//        HashMap<String, Boolean> intersects = new HashMap<String, Boolean>();
//        for (String path1 : paths1)
//        {
//            String[] tags1 = path1.split(" ");
//            String last1 = tags1[tags1.length - 1];
//            intersects.put(last1, true);
//            for (String path2 : paths2)
//            {
//                String first2 = path2.split(" ")[0];
//                intersects.put(first2, true);
//                if (last1.equals(first2))
//                {
//                    paths.add(path1 + " " + path2.substring(first2.length()).trim());
//                }
//            }
//        }
//
//        if (paths.size() != 0 && paths.size() * 1.0 / paths1.size() < 5)
//        {
//            //if(isSamePath(paths))
//            //System.out.println("wwwwww");
//            return paths;
//        }
//
//
//        paths.clear();
//
//        Set<String> tags = intersects.keySet();
//        double maxProb = Double.NEGATIVE_INFINITY;
//        String maxTag = null;
//        for (String tag : tags)
//        {
//            if (maxTag == null)
//            {
//                maxTag = tag;
//            }
//            double curProb = getProb(tag, vf);
//            if (curProb > maxProb)
//            {
//                maxTag = tag;
//                maxProb = curProb;
//            }
//        }
//
//        HashMap<String, Boolean> subpaths1 = new HashMap<String, Boolean>();
//        HashMap<String, Boolean> subpaths2 = new HashMap<String, Boolean>();
//
//
//        for (String path1 : paths1)
//        {
//            String[] tags1 = path1.split(" ");
//            String last1 = tags1[tags1.length - 1];
//            String subpath1 = path1.substring(0, path1.length() - last1.length()).trim();
//            subpaths1.put(subpath1, true);
//        }
//
//        for (String path2 : paths2)
//        {
//            String first2 = path2.split(" ")[0];
//            String subpath2 = path2.substring(first2.length()).trim();
//            subpaths2.put(subpath2, true);
//        }
//
//
//        Set<String> subpaths1_unique = subpaths1.keySet();
//        Set<String> subpaths2_unique = subpaths2.keySet();
//
//        for (String subpath1 : subpaths1_unique)
//        {
//            for (String subpath2 : subpaths2_unique)
//            {
//                paths.add(subpath1 + " " + maxTag + " " + subpath2);
//            }
//        }
//
//        if (paths.size() * 1.0 / paths1.size() > 5)
//        {
//            double maxScore = Double.NEGATIVE_INFINITY;
//            String maxPath2 = null;
//            for (String subpath2 : subpaths2_unique)
//            {
//                Double curScore = m_crss.get(maxTag + " " + subpath2);
//                if (curScore != null && curScore > maxScore)
//                {
//                    maxScore = curScore;
//                    maxPath2 = subpath2;
//                }
//            }
//
//            paths.clear();
//            for (String subpath1 : subpaths1_unique)
//            {
//                paths.add(subpath1 + " " + maxTag + " " + maxPath2);
//            }
//            //if(isSamePath(paths))
//            //System.out.println("wwwwww");
//            return paths;
//        }
//        //if(isSamePath(paths))
//        //System.out.println("wwwwww");
//        return paths;
//    }

    public void train()
    {
        MyTimer timer = new MyTimer();
        timer.start();
        initCPT();
        m_corpus.printStatistics();
        timer.end();
        timer.printTime();
        System.out.println("Train Finished!");
    }
    /*
     * public static void main(String[] args){ Corpus corpus = new
     * Corpus(Global.g_BrwonCorpusFile); //training_corpus.printStatistics();
     * Corpus training_corpus = corpus.genSubCorpus(0, corpus.getNumSentences()
     * - 1000); Corpus decoding_corpus =
     * corpus.genSubCorpus(corpus.getNumSentences() - 1000,
     * corpus.getNumSentences()); CRModel_sp1 model = new
     * CRModel_sp1(training_corpus, Global.g_BrwonModelFile, false);
     * model.train();
	}
     */
}
