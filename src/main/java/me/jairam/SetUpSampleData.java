package me.jairam;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.UUID;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import me.jairam.madness.DocumentParsedException;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;

import com.scriptandscroll.adt.*;


public class SetUpSampleData extends DefaultHandler
{

	private boolean processingAuthorGroup = false;
	private boolean processingAuthor = false;
	private boolean processingDoc = false;
	private boolean processingTitle = false;
	private StringBuilder temp;
	private ArrayList<String> authors;
	
	Keyspace ks;
	ColumnFamily cf;
	
	UUID uuid;
	
	SAXParser sp;
	
	public SetUpSampleData()
	{
		authors = new ArrayList<String>();
		
		ks = new Keyspace("Test Cluster", "DocumentStore", "localhost:9160");
		cf = new ColumnFamily(ks, "Documents");
	}
	

	public void parseDocument(String doc) {

		uuid = UUID.randomUUID();
		
		//get a factory
		SAXParserFactory spf = SAXParserFactory.newInstance();

		try {

			//get a new instance of parser
			SAXParser sp = spf.newSAXParser();
			XMLReader reader = sp.getXMLReader();
			reader.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
		    reader.setFeature("http://xml.org/sax/features/validation", false);
			//parse the file and also register this class for call backs
			sp.parse(doc, this);

		}
		catch(DocumentParsedException doe)
		{
			return;
		}
		catch(SAXException se) {
			se.printStackTrace();
		}catch(ParserConfigurationException pce) {
			pce.printStackTrace();
		}catch (IOException ie) {
			ie.printStackTrace();
		}
	}


	/* (non-Javadoc)
	 * @see org.xml.sax.helpers.DefaultHandler#endElement(java.lang.String, java.lang.String, java.lang.String)
	 */
	@Override
	public void endElement(String uri, String localName, String qName)
			throws SAXException {
		if(qName.equalsIgnoreCase("name") && processingAuthorGroup)
		{
			processingAuthor = false;
		}
		else if(qName.equalsIgnoreCase("contrib-group"))
		{
			processingAuthorGroup = false;
			cf.putColumn(uuid.toString(), new Column("authors",authors.toString()));
			//System.out.println(uuid.toString() + " : " + authors.toString());
			authors.clear();
			if(processingDoc)
			{
				throw new DocumentParsedException();
			}
		}
		else if(qName.equalsIgnoreCase("article-title") && processingDoc)
		{
			processingTitle = false;
			cf.putColumn(uuid.toString(), new Column("title", temp.toString()));
			//System.out.println(uuid.toString() + " : " + temp.toString());
		}
	}

	/* (non-Javadoc)
	 * @see org.xml.sax.helpers.DefaultHandler#startElement(java.lang.String, java.lang.String, java.lang.String, org.xml.sax.Attributes)
	 */
	@Override
	public void startElement(String uri, String localName, String qName,
			Attributes attributes) throws SAXException {
		if(qName.equalsIgnoreCase("contrib") && attributes.getValue("contrib-type").equalsIgnoreCase("author"))
		{
			processingAuthorGroup = true;
			temp = new StringBuilder();
		}
		else if(qName.equalsIgnoreCase("name") && processingAuthorGroup)
		{
			processingAuthor = true;
		}
		else if(qName.equalsIgnoreCase("article-meta"))
		{
			processingDoc = true;
		}
		else if(qName.equalsIgnoreCase("article-title") && processingDoc)
		{
			processingTitle = true;
			temp = new StringBuilder();
		}
	}



	/* (non-Javadoc)
	 * @see org.xml.sax.helpers.DefaultHandler#characters(char[], int, int)
	 */
	@Override
	public void characters(char[] ch, int start, int length)
			throws SAXException {
		String val = new String(ch, start, length);
		if(val.startsWith("\n") || val.startsWith("\t")) return;
		if(processingAuthor && processingAuthorGroup)
		{
			if(temp.length() == 0)
			{
				temp.append(new String(ch, start, length));
			}
			else
			{
				temp.append(' ');
				temp.append(new String(ch, start, length));
				authors.add(temp.toString());
			}
		}
		
		if(processingDoc && processingTitle)
		{
			temp.append(new String(ch, start, length));
		}
	}


	/**
	 * @param args
	 */
	public static void main(String[] args) {
		SetUpSampleData s = new SetUpSampleData();
		
		File file = new File("/home/jairam/workspace/Documents");
		for(File dir: file.listFiles())
		{
			for(File f: dir.listFiles())
			{
				s.parseDocument(f.getAbsolutePath());
			}
		}
		
		System.exit(0);
	}
}
