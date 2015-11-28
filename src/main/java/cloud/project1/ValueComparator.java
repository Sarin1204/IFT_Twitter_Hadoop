package cloud.project1;

import java.util.Comparator;
import java.util.Map;

public class ValueComparator implements Comparator<String>{

	Map<String,Integer> mp;
	public ValueComparator(Map<String,Integer> mp)
	{
		this.mp=mp;
	}
	
	@Override
	public int compare(String arg0, String arg1) {
		if(mp.get(arg0)>=mp.get(arg1))
		{
			return -1;			
		}
		else
		{
			return 1;
		}
	}

	
}
