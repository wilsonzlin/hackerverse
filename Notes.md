Crawl:
- Users
  - Bio
  - Favourite comments
  - Favourite posts
- Comments
  - Text
  - Timestamp
  - Votes
  - Author
- Posts
  - Title
  - Timestamp
  - Votes
  - Author
Filter:
- Dead/flagged posts
- Dead/flagged comments
- Users that have excessive dead/flagged posts or comments
- Posts with <3 upvotes ("objectively uninteresting/bad")
- Users that have commented on less than 5 posts (too small/inaccurate sample size)
Sources of positive examples:
- User submitted
- User favourites
- User upvotes, if publicly available
Sampling negatives:
- Fixed amount (i.e. don't sample every single non-positive post)
- Lean towards posts that are far away from user's positives in the semantic space
- Lean towards less upvoted posts (but don't *only* pick unpopular posts)
- Add some randomness
  - Remember: we don't *actually* know what they don't like
Model inputs:
- Embeddings of HN titles
  - We don't have horsepower to crawl and embed all the actual contents (yet)
    - However, I'd be willing to bet that titles alone would go really far. Just consider: titles must be <60 characters, so they're optimised for dense clear interesting representations, and a lot of people don't even read the article and engage purely with the title itself.
- Time of post
  - Will likely be irrelevant to most interactions, but *might* be for some (e.g. sensitive to recency)
  - Not sure how to represent this, or what this should represent.
    - Actual age of article relative to posting (e.g. those posts with `(YYYY)` at the end of the title).
      - Maybe this represents timelessness of the content, idea, topic, etc.?
      - This could also represent the inverse e.g. random news articles, that aren't interesting after the event's passed.
    - Time of comment relative to post?
    - Time of the post, in absolute terms?
- Embedding of author's bio
  - Mostly sparse (most users won't have anything), but once again *might* be useful for some
- It *may* be worthwhile generating *some representation* (not necessarily current text embeddings, given their focus on content and not meta/style/etc.) of a user's comments and their favourited comments
  - This may have benefits if they use comments to express their ideas and personalities authentically and clearly.
