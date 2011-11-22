package me.jairam;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import org.json.JSONArray;
import org.json.JSONException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import me.jairam.madness.DocumentParsedException;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;

import com.scriptandscroll.adt.*;

import me.jairam.utils.security.*;

public class SetUpSampleData extends DefaultHandler
{

	private boolean processingAuthorGroup = false;
	private boolean processingAuthor = false;
	private boolean processingDoc = false;
	private boolean processingTitle = false;
	private StringBuilder temp;
	private HashMap<String, String> authors;
	private String title;
	
	private ArrayList<String> titleKeys;

	Keyspace ks;
	ColumnFamily documentCf, authorCf, userCf;

	SAXParser sp;

	public SetUpSampleData()
	{
		authors = new HashMap<String, String>();

		ks = new Keyspace("Test Cluster", "DocumentStore", "localhost:9160");
		documentCf = new ColumnFamily(ks, "Documents");
		authorCf = new ColumnFamily(ks, "Authors");
		userCf = new ColumnFamily(ks, "Users");
		titleKeys = new ArrayList<String>();
	}


	public void parseDocument(String doc) {

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

			String key = UUID.randomUUID().toString();
			
			titleKeys.add(key);

			documentCf.putColumn(key, "title", title);
			documentCf.putColumn(key, "authors",authors.toString());

			for(Map.Entry<String, String> author: authors.entrySet())
			{
				key = author.getKey();
				
				Row row = authorCf.getRow(key, "", "");
				if(row.isEmpty())
				{
					authorCf.putColumn(key, "author", author.getValue());
					authorCf.putColumn(key, "title", title);
				}
				else
				{
					try 
					{
						JSONArray titles = new JSONArray(row.getColumnValue("title"));
						titles.put(title);
						authorCf.putColumn(key, "title", titles.toString());
					} 
					catch (JSONException e) 
					{
						e.printStackTrace();
					}
				}
			}


			authors.clear();
			if(processingDoc)
			{
				throw new DocumentParsedException();
			}
		}
		else if(qName.equalsIgnoreCase("article-title") && processingDoc)
		{
			processingTitle = false;
			title = temp.toString();
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
				authors.put(UUID.randomUUID().toString(), temp.toString());
			}
		}

		if(processingDoc && processingTitle)
		{
			temp.append(new String(ch, start, length));
		}
	}


	public void addUsers(int numOfUsers, String namesFiles) throws IOException
	{
		BufferedReader br = new BufferedReader(new FileReader(namesFiles));
		
		Random numberOfBooksGenerator = new Random();
		Random titleKeyNumberGenerator = new Random();
		
		int numberOfTitles = titleKeys.size();
		
		for(int i=0; i < numOfUsers; i++)
		{
			ArrayList<String> list = new ArrayList<String>();
			int numberOfBooksForUser = numberOfBooksGenerator.nextInt(10)+1;
			for(int j=0; j < numberOfBooksForUser; j++)
			{
				list.add(titleKeys.get(titleKeyNumberGenerator.nextInt(numberOfTitles)));
			}
		
			String key = UUID.randomUUID().toString();
			userCf.putColumn(key, "name", br.readLine());
			
			StringBuilder sb = new StringBuilder();
			for(String book: list)
			{
				sb.append(book);
				sb.append(",");
			}
			sb.deleteCharAt(sb.length()-1);
			userCf.putColumn(key, "books", sb.toString());
			
		}
		br.close();
	}
	
	
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		SetUpSampleData s = new SetUpSampleData();

		String documentFile = args[0];
		String usernameFile = args[1];
		
		long size = 0;
		File file = new File(documentFile);
		for(File dir: file.listFiles())
		{
			for(File f: dir.listFiles())
			{
				size += f.length();
				s.parseDocument(f.getAbsolutePath());
			}
			if(size > (25*1024*1024)) break;
		}
		
		
		s.addUsers(100, usernameFile);

		System.exit(0);
	}
}
