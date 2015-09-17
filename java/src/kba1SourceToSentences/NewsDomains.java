package kba1SourceToSentences;

import io.github.htools.search.ByteRegex;
import io.github.htools.search.ByteSearchPosition;
import io.github.htools.io.Datafile;
import io.github.htools.io.ResourceDataIn;
import io.github.htools.lib.Log;

/**
 * assigns a domain ID, based on a URL, from a list of domains used
 * @author jeroen
 */
public class NewsDomains {

    public static final Log log = new Log(NewsDomains.class);
    // resource file that contains a list of domains used, with a pseudo regex
    // that is used to determine if a page within a domain is indeed a 
    // news article
    private static final String defaultResourceFile = "newssites_kba.txt";
    
    private static NewsDomains instance;
    // list of hosts for domainID 
    private String[] host;
    // all regex for the domains combined into one expression, that returns a 
    // pattern number to indicate which domain matched, used as internal domain ID
    private ByteRegex domainPattern;

    public NewsDomains(String resourcefile) {
        domainPattern = createDomainPattern(resourcefile);
    }

    private NewsDomains() {
        this(defaultResourceFile);
    }

    public static NewsDomains getInstance() {
        if (instance == null) {
            instance = new NewsDomains();
        }
        return instance;
    }
    
    private ByteRegex createDomainPattern(String resourcefilename) {
        // read entire files of domainrules
        String domainRules = getDatafile(resourcefilename).readAsString();
        // split in separate domainrules, every line one rule
        String[] domainRule = domainRules.split("\n");
        // setup host array
        host = new String[domainRule.length];

        // construct a regex that identifies if a page within a domain is a
        // news article. In tje resource file, %W, %A, %N, %Y are used to
        // express different wildcard matches, see below. In the resource
        // file, a . is a literal period and a dash a literal dash.
        ByteRegex[] regex = new ByteRegex[domainRule.length];
        for (int i = 0; i < domainRule.length; i++) {
            String rule = domainRule[i];
            // host is the host section of the domain rule
            host[i] = rule.substring(0, rule.indexOf('/'));
            rule = rule.replace("-", "\\-"); // convert dash to literal dash
            rule = rule.replace(".", "\\."); // convert . to a literal .
            rule = rule.replace("%W", "[^\\?]*?"); // any character except a ?
            rule = rule.replace("%A", ".*?"); // any character
            rule = rule.replace("%N", "[^/\\?]*"); // any text except a / or a ?
            rule = rule.replace("%Y", "201\\d"); // hack yo detect a year, only works for years 201x
            if (rule.length() > 0) {
                // create regex of the rule
                regex[i] = new ByteRegex(rule);
            }
        }
        return ByteRegex.combine(regex);
    }

    private Datafile getDatafile(String resourcefilename) {
        return new Datafile(new ResourceDataIn(NewsDomains.class, "resources/" + resourcefilename));
    }

    private String[] getDomains() {
        return host;
    }

    /**
     * @param domainID
     * @return host String for given internal domain ID
     */
    public String getHostPart(int domainID) {
        return host[domainID];
    }

    /**
     * @param url
     * @return internal domain number for given URL, or -1 if domain is unknown
     */
    public int getDomainForUrl(String url) {
        ByteSearchPosition urlPosition = domainPattern.findPos(url);
        return urlPosition.found() ? urlPosition.pattern : -1;
    }
    
    public static void main(String[] args) {
        String[] domains = new NewsDomains().getDomains();
        for (int i = 0; i < domains.length; i++) {
            log.printf("%d %s", i, domains[i]);
        }
    }
}
