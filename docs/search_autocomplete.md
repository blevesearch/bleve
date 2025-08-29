# Edge N-gram Autocomplete in Bleve

Search autocomplete is a feature which we see in search boxes when suggestions appear while we type.
So when we type `jav`, we see suggestions like: `java` `javascript` `javascript programming` etc.
This is helpful because it saves users time in finding what they are looking for.

![Alt Text](/docs/images/search_autocomplete.png "search autocomplete suggestion")

## 2. How Does It Work?

Autocomplete generally works in three steps:

1. **Index Time**: Breaking text into searchable pieces (tokens)
2. **Query Time**: Matching user input against indexed tokens  
3. **Results**: Returning relevant suggestions quickly

But before we jump into the flow, let's understand different methods to achieve this and why edge n-grams are the most efficient approach.

## 3. Different Tokenization Methods

There are several tokenization approaches, each with its own strengths and weaknesses:

### 3.1 Single Token Tokenizer

```go
// analysis/tokenizer/single/single.go
func (t *SingleTokenTokenizer) Tokenize(input []byte) analysis.TokenStream {
    return analysis.TokenStream{
        &analysis.Token{
            Term:     input,    // Here entire input as one token
            Position: 1,
            Start:    0,
            End:      len(input),
            Type:     analysis.AlphaNumeric,
        },
    }
}
```

**How it works**: Treats the entire input as a single token.

**Example**: "JavaScript Programming" → [`"JavaScript Programming"`]

**Pros**:

- Simple and fast
- Perfect for exact phrase matching
- Minimal index size

**Cons**:

- No autocomplete support (can't match partial text)
- Not flexible for search

**Use case**: Keyword fields, IDs, exact phrase matching

### 3.2 Whitespace Token Tokenizer

```go
// analysis/tokenizer/whitespace/whitespace.go  
func TokenizerConstructor(config map[string]interface{}, cache *registry.Cache) (analysis.Tokenizer, error) {
    return character.NewCharacterTokenizer(notSpace), nil
}

func notSpace(r rune) bool {
    return !unicode.IsSpace(r)  // Split on whitespace
}
```

**How it works**: Splits text on whitespace characters.

**Example**: "JavaScript Programming" → [`"JavaScript"`, `"Programming"`]

**Pros**:

- Simple word-based tokenization
- Works well for basic prefix search

**Cons**:

- Only matches from word beginnings
- No support for partial word matching
- Limited autocomplete capabilities

**Use case**: Basic search, word-level indexing

### 3.3 N-gram Method

```go
// analysis/token/ngram/ngram.go
func (s *NgramFilter) Filter(input analysis.TokenStream) analysis.TokenStream {
    rv := make(analysis.TokenStream, 0, len(input))
    
    for _, token := range input {
        runeCount := utf8.RuneCount(token.Term)
        runes := bytes.Runes(token.Term)
        
        // ..generate all possible n-grams
    }
    return rv
}
```

**How it works**: Creates ALL possible substrings of specified lengths.

**Example**: "java" with 2-3 grams → [`"ja"`, `"av"`, `"va"`, `"jav"`, `"ava"`]

**Pros**:

- Supports partial matching anywhere in the text
- Coverage of all possible substrings

**Cons**:

- **MASSIVE index size** (exponential growth)
- Brings more noise as irrelevant matches (e.g., "av" matching "java")
- Poor performance for autocomplete
- High memory usage

**Use case**: Full-text substring search (not ideal for autocomplete)

### 3.4 Edge N-gram Method (preferred for autocomplete)

```go
// analysis/token/edgengram/edgengram.go
func (s *EdgeNgramFilter) Filter(input analysis.TokenStream) analysis.TokenStream {
    rv := make(analysis.TokenStream, 0, len(input))
    
    for _, token := range input {
        runeCount := utf8.RuneCount(token.Term)
        runes := bytes.Runes(token.Term)
        // ..builds tokens based from either end, specified in the input
    }
    return rv
}
```

**How it works**: Creates substrings only from the beginning (or end) of each word.

**Example**: "javascript" with front edge n-grams (1-5) → [`"j"`, `"ja"`, `"jav"`, `"java"`, `"javas"`]

**Pros**:

- Perfect for autocomplete (matches prefixes naturally)
- Efficient index size (linear growth vs exponential)  
- Fast queries (direct term matching, no complex processing)
- Intuitive results (matches what users expect)
- Highly scalable for large datasets

**Cons**:

- Only supports prefix matching (but that's preferred for autocomplete!)
- Slightly larger index than basic tokenization

**Use case**: Search autocomplete, prefix-based search

## 4. Why Edge N-grams Are Most Efficient for Autocomplete

Let's see what happens when a user types "java" with edge_ngram tokenizer:

```text
Index contains: ["j", "ja", "jav", "java", "javas", "javasc", "javascr", ...]
User types: "java"  
Query: ExactTermQuery("java")
Process: Direct hash table lookup for term "java"
Result: Instant match, then retrieve documents containing this term
```

**Key advantages of Edge N-gram approach:**

1. **O(1) lookup**: Direct term matching
2. **No query-time processing**: Terms are pre-computed at index time  
3. **Better caching**: Exact term queries cache better than prefix queries
4. **Consistent performance**: Query time doesn't increase with index size

## 5. Step-by-Step Implementation: Building Search Autocomplete

Now let's see how to implement this in practice using our exact configuration. We'll build a complete autocomplete system step by step.

### Step 1: Create Custom Edge N-gram Token Filter

First, we need to create a custom token filter for edge n-grams. Here's how it looks in our configuration:

```go
// Create a new index mapping
indexMapping := mapping.NewIndexMapping()

// 1. Define the edgeGram token filter
edgeGramFilter := map[string]interface{}{
    "type": edgengram.Name,
    "min":  2.0,
    "max":  4.0,
    "back": false,
}

// Register the token filter
if err := indexMapping.AddCustomTokenFilter("Engram", edgeGramFilter); err != nil {
    log.Fatal(err)
}
```

**What each setting does:**

- `"type": "edge_ngram"` - Tells Bleve to use the edge n-gram filter
- `"min": 2` - Start creating tokens from 2 characters ("ja", "sc", etc.)
- `"max": 4` - Stop at 4 characters ("java", "scri", etc.)
- `"back": "false"` - Create tokens from the front (beginning) of words

### Step 2: Create Custom Analyzer

Next, we create an analyzer that uses our custom token filter along with other helpful filters:

```go
// 2. Define a custom analyzer that uses it
customAnalyzer := map[string]interface{}{
    "type":      custom.Name,
    "tokenizer": "unicode",
    "char_filters": []string{
         zerowidthnonjoiner.Name,
    },
    "token_filters": []string{
        "Engram", // our custom edge_ngram filter
        "to_lower",
        "stop_en",
    },
}

if err := indexMapping.AddCustomAnalyzer("edgeGramAnalyzer", customAnalyzer); err != nil {
    log.Fatal(err)
}
```

**The pipeline works like this:**

1. **Input Text**: "Schaumbergfest Event"

2. **Tokenizer** (`unicode`): Splits into words

   ```text
   ["Schaumbergfest", "Event"]
   ```

3. **Character Filter** (`zero_width_spaces`): Removes invisible characters

   ```text
   ["Schaumbergfest", "Event"] (cleaned)
   ```

4. **Token Filter 1** (`Engram`): Creates edge n-grams (2-4 chars)

   ```text
   ["Sc", "Sch", "Scha", "Ev", "Eve", "Even"]
   ```

5. **Token Filter 2** (`to_lower`): Makes everything lowercase

   ```text
   ["sc", "sch", "scha", "ev", "eve", "even"]
   ```

6. **Token Filter 3** (`stop_en`): Removes common words (none in this case)

   ```text
   ["sc", "sch", "scha", "ev", "eve", "even"] (final tokens)
   ```

### Step 3: Configure Field Mapping

Now we tell Bleve which fields to apply our autocomplete analyzer to:

```go
    // 3. Assign analyzer to a field mapping
    fieldMapping := mapping.NewTextFieldMapping()
    fieldMapping.Analyzer = "edgeGramAnalyzer"
    
    indexMapping.DefaultMapping.AddFieldMappingsAt("title", fieldMapping)
    
    indexPath := "example.bleve"
    index, err := bleve.New(indexPath, indexMapping)
    if err != nil {
        log.Fatal(err)
    }
```

### Step 4: How It Works in Real Search

When someone searches for "sc", here's what happens:

**Index contains these tokens:**

```text
"sc" → [document1: "Schaumbergfest", document2: "Script", ...]
"sch" → [document1: "Schaumbergfest", ...]  
"scha" → [document1: "Schaumbergfest", ...]
```

**User types "sc":**

1. Query: `name:sc`
2. Bleve looks up exact term "sc" in the index
3. Finds document with "Schaumbergfest" and "Script"
4. Returns suggestion instantly

```go

type Document struct {
  ID    string `json:"id"`
  Title string `json:"title"`
}
// 4. Index Documents
documents := []Document{
  {
    ID:    "doc1",
    Title: "Schaumbergfest",
  },
  {
    ID:    "doc2",
    Title: "Script",
  },
}

batch := index.NewBatch()
for _, doc := range documents {
  batch.Index(doc.ID, doc)
}
if err := index.Batch(batch); err != nil {
  log.Fatal(err)
}

// 5. Search the created index
query := bleve.NewMatchQuery("sc")
query.SetField("title")
searchRequest := bleve.NewSearchRequest(query)
searchRequest.Explain = true
searchRequest.Fields = []string{"title"}
searchResult, err := index.Search(searchRequest)
if err != nil {
  log.Fatal(err)
}
fmt.Println(searchResult)
```

Output:

```bash

$ go run main.go

2 matches, showing 1 through 2, took 189.041µs
    1. doc2 (0.343255)
        title
                Script
    2. doc1 (0.343255)
        title
                Schaumbergfest
```

Note: To run code, enclose code starting from Step 1 in func main.
