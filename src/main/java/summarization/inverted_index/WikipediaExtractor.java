package summarization.inverted_index;

import org.apache.commons.text.StringEscapeUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import utils.XMLParser;

import java.io.IOException;
import java.util.Map;

/**
 * This class builds an inverted index, of Wikipedia URLs in peoples' posts, to a set of answer post IDs.
 * See src/resources/Posts.xml for data
 */
public class WikipediaExtractor {

    private static String getWikipediaURL(String text) {

        int index = text.indexOf("\"http://en.wikipedia.org");
        if (index == -1)
            return null;

        int indexEnd = text.indexOf('"', index + 1);

        if (indexEnd == -1) return null;

        int indexHash = text.indexOf('#', index + 1);

        if (indexHash != -1 && indexHash < indexEnd) return text.substring(index + 1, indexHash);
        else return text.substring(index + 1, indexEnd);

    }


    public static class WikipediaExtractorMapper extends Mapper<Object, Text, Text, Text> {

        private Text link = new Text();
        private Text outKey = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            Map<String, String> map = XMLParser.transformXmlToMap(value.toString());

            String body = map.get("Body");
            String postType = map.get("PostTypeId");
            String row_id = map.get("Id");

            if(body == null || row_id == null || postType == null || postType.equals("1"))
                return;

            body = StringEscapeUtils.unescapeHtml3(body.toLowerCase());

            String wikipedia = getWikipediaURL(body);
            if(wikipedia == null)
                return;
            link.set(wikipedia);
            outKey.set(row_id);
            context.write(link, outKey);

        }
    }

    public static class WikipediaExtractorReducer extends Reducer<Text, Text, Text, Text> {

        private Text result = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            StringBuilder builder = new StringBuilder();

            for(Text id: values)
                builder.append(id.toString()).append(" ");

            result.set(builder.substring(0, builder.length()-1));
            context.write(key, result);
        }
    }

}
