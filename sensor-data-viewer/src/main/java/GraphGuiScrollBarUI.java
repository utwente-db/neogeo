import java.awt.Color;
import java.awt.Component;
import java.awt.Graphics;
import java.awt.Point;
import java.awt.Rectangle;
import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;
import java.awt.event.MouseMotionListener;

import javax.swing.BoundedRangeModel;
import javax.swing.JComponent;
import javax.swing.JScrollBar;
import javax.swing.JScrollPane;
import javax.swing.JViewport;
import javax.swing.event.MouseInputAdapter;
import javax.swing.plaf.ComponentUI;
import javax.swing.plaf.metal.MetalScrollBarUI;

public class GraphGuiScrollBarUI extends MetalScrollBarUI {
	public static int resizeOffset = 5;
	// Create our own scrollbar UI!
	public static ComponentUI createUI( JComponent c ) {
		return new GraphGuiScrollBarUI();
	}

	/**
	 *  checks whether the current point p is in the area of the thumb 
	 *  to increase the thumb
	 * @param p point where the mouse has been pushed
	 * @return whether the pointer p is in the increase part of the thumb
	 */
	public boolean checkUpIncrease(Point p){
		Rectangle r = this.getThumbBounds();
		if (scrollbar.getOrientation() == JScrollBar.VERTICAL){
			return r.getX()<=p.getX() && r.getY()<=p.getY() &&
			r.getX()+r.getWidth()>=p.getX() && r.getY()+resizeOffset>=p.getY();
		} else{
			return r.getX()<=p.getX() && r.getY()<=p.getY() &&
			r.getX()+resizeOffset>=p.getX() && r.getY()+r.getHeight()>=p.getY();
		}
	}

	/**
	 *  checks whether the current point p is in the area of the thumb 
	 *  to decrease the thumb
	 * @param p point where the mouse has been pushed
	 * @return whether the pointer p is in the decrease part of the thumb
	 */
	public boolean checkDownIncrease(Point p){
		Rectangle r = this.getThumbBounds();
		if (scrollbar.getOrientation() == JScrollBar.VERTICAL){
			return r.getX()<=p.getX() && r.getY()+r.getHeight()-1-resizeOffset<=p.getY() &&
			r.getX()+r.getWidth()>=p.getX() && r.getY()+r.getHeight()>=p.getY();
		} else {
			return r.getX()+r.getWidth()-1-resizeOffset<=p.getX() && r.getY()<=p.getY() &&
			r.getX()+r.getWidth()>=p.getX() && r.getY()+r.getHeight()>=p.getY();
		}
	}

	/**
	 * install the new listener to the scrollbar
	 */
	@Override
	protected void installListeners(){
		super.installListeners();
		GraphGuiMouseListener l = new GraphGuiMouseListener(scrollbar);
		scrollbar.addMouseListener(l);
		scrollbar.addMouseMotionListener(l);
	}

	/** 
	 * This method paints the scroll thumb.  
	 * 
	 * We've just taken the MetalScrollBarUI code and stripped out all the
	 * interesting painting code, replacing it with code that paints a box
	 * and the stripes at the rims of the box.
	 * @param g
	 * @param c
	 * @param thumbBounds
	 */
	@Override
	protected void paintThumb(Graphics g, JComponent c, Rectangle thumbBounds)
	{
		if (!c.isEnabled()) { return; }

		g.translate( thumbBounds.x, thumbBounds.y );
		if ( scrollbar.getOrientation() == JScrollBar.VERTICAL ) {
			if ( !isFreeStanding ) {
				thumbBounds.width += 2;
			}
			g.setColor( Color.gray );
			g.fillRect( 0, 0, thumbBounds.width - 2, thumbBounds.height - 1 );
			g.setColor( Color.black );
			g.fillRect( 0, 0, thumbBounds.width - 2, resizeOffset );
			g.fillRect(0, thumbBounds.height - 1 - resizeOffset , thumbBounds.width - 2, resizeOffset );
			if ( !isFreeStanding ) {
				thumbBounds.width -= 2;
			}
		}
		else  { // HORIZONTAL
			if ( !isFreeStanding ) {
				thumbBounds.height += 2;
			}
//			g.setColor( Color.black );
//			g.fillRect( 0, 0, thumbBounds.width - 1, thumbBounds.height - 2 );
			g.setColor( Color.gray );
			g.fillRect( 0, 0, thumbBounds.width - 1, thumbBounds.height - 2 );
			g.setColor( Color.black );
			g.fillRect( 0, 0, resizeOffset, thumbBounds.height - 2 );
			g.fillRect(thumbBounds.width - 2 -resizeOffset , 0 , resizeOffset, thumbBounds.height - 2 );

			if ( !isFreeStanding ) {
				thumbBounds.height -= 2;
			}
		}
		g.translate( -thumbBounds.x, -thumbBounds.y );
	}  

	/**
	 * this class actually implements a new listener for the mouse actions
	 * 
	 * it ensures that a click on the rims of the thumb actually increases or
	 * decreases the thumb
	 * @author wombachera
	 *
	 */
	public class GraphGuiMouseListener extends MouseInputAdapter {
		private MouseListener[] mouseListeners=null;
		private MouseMotionListener[] mouseMotionListeners;
		private JScrollBar bar = null;
		private GraphGuiScrollBarUI mgr;
		private boolean flagTop = false;
		private boolean flagBottom = false;
		private int offset=0;
		private int scrollBarValue;
		boolean active ;
		
		public GraphGuiMouseListener(JScrollBar bar){
			this.bar=bar;
			this.mouseListeners=bar.getMouseListeners();
			this.mouseMotionListeners=bar.getMouseMotionListeners();
			Component[] comp = bar.getComponents();
			mgr = (GraphGuiScrollBarUI)bar.getLayout();
			for(MouseListener l:mouseListeners)
				if(l!=this)
					bar.removeMouseListener(l);
			for(MouseMotionListener l:mouseMotionListeners)
				if(l!=this)
					bar.removeMouseMotionListener(l);
			active = mgr.isThumbRollover();
		}

		public void mousePressed(MouseEvent e) {
			if(mgr.checkUpIncrease(e.getPoint())){
				// System.out.println("inside up");
				flagTop = true;
				flagBottom = false;
//				e.getY();
			} else if(mgr.checkDownIncrease(e.getPoint())){
				// System.out.println("inside down");
				flagTop = false;
				flagBottom = true;
//				e.getY();
			} else {
				flagTop = false;
				flagBottom = false;
				for(MouseListener l:mouseListeners)
					l.mousePressed(e);
			}
		}

		public void mouseDragged(MouseEvent e) {
			int value;
			int extent;
			if(flagTop){
				value=getValueTopFrom(e);
				extent = bar.getVisibleAmount()+bar.getValue()-value;
				bar.setValues(value, extent, bar.getMinimum(), bar.getMaximum());
				setThumbRollover(active);
				//				System.out.println("value : "+bar.getValue()+"   extend:"+bar.getVisibleAmount());
			} else if (flagBottom){
				value=getValueBottomFrom(e);
//				extent = value-bar.getValue();
//				bar.setValues(bar.getValue(), extent, bar.getMinimum(), bar.getMaximum());
				extent = bar.getVisibleAmount()+value-bar.getValue();
				bar.setValues(bar.getValue(), extent, bar.getMinimum(), bar.getMaximum());
				//				bar.setVisibleAmount(extent);
				setThumbRollover(active);
				//				System.out.println("value : "+value+"    bar:"+bar.getValue()+"   end:"+(bar.getVisibleAmount()+bar.getValue()));
			} else {
				for(MouseMotionListener l:mouseMotionListeners)
					l.mouseDragged(e);
			} 
		}



		public void mouseReleased(MouseEvent e) {
			if(!flagTop && !flagBottom){
				for(MouseListener l:mouseListeners)
					l.mouseReleased(e);
			} 
		}

		private int getValueTopFrom(MouseEvent e) {
			BoundedRangeModel model = bar.getModel();
			Rectangle thumbR = mgr.getThumbBounds();
			float trackLength;
			int thumbMin, thumbMax, thumbPos;

			if (bar.getOrientation() == JScrollBar.VERTICAL) {
				thumbMin = decrButton.getY() + decrButton.getHeight();
				thumbMax = incrButton.getY() - thumbR.height;
				thumbPos = Math.min(thumbMax, Math.max(thumbMin, (e.getY() - offset)));
				setThumbBounds(thumbR.x, thumbPos, thumbR.width, thumbR.height);
				trackLength = getTrackBounds().height;
			}
			else {
				if (bar.getComponentOrientation().isLeftToRight()) {
					thumbMin = decrButton.getX() + decrButton.getWidth();
					thumbMax = incrButton.getX() - thumbR.width;
				} else {
					thumbMin = incrButton.getX() + incrButton.getWidth();
					thumbMax = decrButton.getX() - thumbR.width;
				}
				thumbPos = Math.min(thumbMax, Math.max(thumbMin, (e.getX() - offset)));
				setThumbBounds(thumbPos, thumbR.y, thumbR.width, thumbR.height);
				trackLength = getTrackBounds().width;
			}

			/* Set the scrollbars value.  If the thumb has reached the end of
			 * the scrollbar, then just set the value to its maximum.  Otherwise
			 * compute the value as accurately as possible.
			 */
			if (thumbPos == thumbMax) {
				if (bar.getOrientation() == JScrollBar.VERTICAL ||
						bar.getComponentOrientation().isLeftToRight()) {
					return model.getMaximum() - model.getExtent();
				} else {
					return model.getMinimum();
				}
			}
			else {
				float valueMax = model.getMaximum() - model.getExtent();
				float valueRange = valueMax - model.getMinimum();
				float thumbValue = thumbPos - thumbMin;
				float thumbRange = thumbMax - thumbMin;
				int value;
				if (bar.getOrientation() == JScrollBar.VERTICAL ||
						bar.getComponentOrientation().isLeftToRight()) {
					value = (int)(0.5 + ((thumbValue / thumbRange) * valueRange));
				} else {
					value = (int)(0.5 + (((thumbMax - thumbPos) / thumbRange) * valueRange));
				}

				scrollBarValue = value + model.getMinimum();
				return scrollBarValue;
			}
		}
		private int getValueBottomFrom(MouseEvent e) {
			BoundedRangeModel model = bar.getModel();
			Rectangle thumbR = mgr.getThumbBounds();
			float trackLength;
			int thumbMin, thumbMax, thumbPos;

			if (bar.getOrientation() == JScrollBar.VERTICAL) {
				thumbMin = decrButton.getY() + decrButton.getHeight();
				thumbMax = incrButton.getY() - thumbR.height;
				thumbPos = Math.min(thumbMax, Math.max(thumbMin, (e.getY() - offset- thumbR.height)));
				setThumbBounds(thumbR.x, thumbPos, thumbR.width, thumbR.height);
				trackLength = getTrackBounds().height;
			}
			else {
				if (bar.getComponentOrientation().isLeftToRight()) {
					thumbMin = decrButton.getX() + decrButton.getWidth();
					thumbMax = incrButton.getX() - thumbR.width;
				} else {
					thumbMin = incrButton.getX() + incrButton.getWidth();
					thumbMax = decrButton.getX() - thumbR.width;
				}
				thumbPos = Math.min(thumbMax, Math.max(thumbMin, (e.getX() - offset - thumbR.width)));
				setThumbBounds(thumbPos, thumbR.y, thumbR.width, thumbR.height);
				trackLength = getTrackBounds().width;
			}

			/* Set the scrollbars value.  If the thumb has reached the end of
			 * the scrollbar, then just set the value to its maximum.  Otherwise
			 * compute the value as accurately as possible.
			 */
			if (thumbPos == thumbMax) {
				if (bar.getOrientation() == JScrollBar.VERTICAL ||
						bar.getComponentOrientation().isLeftToRight()) {
					return model.getMaximum() - model.getExtent();
				} else {
					return model.getMinimum();
				}
			}
			else {
				float valueMax = model.getMaximum() - model.getExtent();
				float valueRange = valueMax - model.getMinimum();
				float thumbValue = thumbPos - thumbMin;
				float thumbRange = thumbMax - thumbMin;
				int value;
				if (bar.getOrientation() == JScrollBar.VERTICAL ||
						bar.getComponentOrientation().isLeftToRight()) {
					value = (int)(0.5 + ((thumbValue / thumbRange) * valueRange));
				} else {
					value = (int)(0.5 + (((thumbMax - thumbPos) / thumbRange) * valueRange));
				}

				scrollBarValue = value + model.getMinimum();
				return scrollBarValue;
			}
		}

	}

}