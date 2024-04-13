use ahash::AHashSet;
use chrono::DateTime;
use chrono::Utc;
use itertools::Itertools;
use once_cell::sync::Lazy;
use scraper::ElementRef;
use scraper::Html;
use scraper::Selector;
use serde::Serialize;

static SEL_HEAD_TITLE: Lazy<Selector> = Lazy::new(|| Selector::parse("head > title").unwrap());

static SEL_META: Lazy<Selector> = Lazy::new(|| Selector::parse("meta").unwrap());

static SEL_BODY: Lazy<Selector> = Lazy::new(|| Selector::parse("body").unwrap());

static SEL_MAIN_ARTICLE: Lazy<Selector> = Lazy::new(|| {
  // Copied from wilsonzlin/crawler-toolkit-web.
  Selector::parse(
    r#"
      main article,
      #article,
      [role=article],
      [itemtype=http://schema.org/Article],
      [itemtype=https://schema.org/Article]
    "#,
  )
  .unwrap()
});

static SEL_SNIPPET_STRIP: Lazy<Selector> = Lazy::new(|| {
  // Copied from wilsonzlin/crawler-toolkit-web.
  Selector::parse(
    r#"
      blockquote,
      figure,
      h1,
      header,
      table
    "#,
  )
  .unwrap()
});

static SEL_STRIP: Lazy<Selector> = Lazy::new(|| {
  // Copied from wilsonzlin/crawler-toolkit-web.
  Selector::parse(
    r#"
      [aria-hidden],
      [hidden],
      [role=alert],
      [role=alertdialog],
      [role=button],
      [role=checkbox],
      [role=combobox],
      [role=complementary],
      [role=feed],
      [role=menu],
      [role=menubar],
      [role=navigation],
      [role=none],
      [role=note],
      [role=presentation],
      [role=search],
      [role=searchbox],
      [role=tablist],
      [role=toolbar],
      [role=tooltip],
      [role=tree],
      [role=treegrid],
      aside,
      button,
      canvas,
      dialog,
      footer,
      form,
      hr,
      input,
      label,
      link,
      menu,
      meta,
      nav,
      noscript,
      object,
      option,
      progress,
      script,
      select,
      style,
      svg,
      template,
      title
    "#,
  )
  .unwrap()
});

// Value stored in the `kv` table at key `url/$ID/meta`.
#[derive(Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Meta {
  description: Option<String>,
  image_url: Option<String>,
  lang: Option<String>,
  snippet: Option<String>,
  timestamp: Option<DateTime<Utc>>,
  timestamp_modified: Option<DateTime<Utc>>,
  title: Option<String>,
}

// Copied from wilsonzlin/crawler-toolkit-web.
static INLINE_ELEMS: Lazy<AHashSet<&'static str>> = Lazy::new(|| {
  AHashSet::from([
    "a", "abbr", "acronym", "audio", "b", "bdi", "bdo", "big", "button", "canvas", "cite", "code",
    "data", "datalist", "del", "dfn", "em", "embed", "i", "iframe", "img", "input", "ins", "kbd",
    "label", "map", "mark", "math", "meter", "noscript", "object", "output", "picture", "progress",
    "q", "ruby", "s", "samp", "script", "select", "slot", "small", "span", "strong", "sub", "sup",
    "svg", "template", "textarea", "time", "tt", "u", "var", "video", "wbr",
  ])
});

// Copied from wilsonzlin/crawler-toolkit-web.
fn element_to_text<'a>(elem: ElementRef<'a>, emit_link_hrefs: bool) -> String {
  let mut out = String::new();
  fn visit<'a>(out: &mut String, elem: ElementRef<'a>, emit_link_hrefs: bool) {
    let tag_name = elem.value().name();
    if tag_name == "br" {
      out.push('\n');
      return;
    };
    // For full accuracy, we need to insert newlines both before and after the block element, in case the element before/after isn't a block element.
    if !INLINE_ELEMS.contains(tag_name) {
      out.push_str("\n\n");
    };
    let should_emit_href = emit_link_hrefs && tag_name == "a" && elem.attr("href").is_some();
    if should_emit_href {
      out.push_str("[");
    };
    for c in elem.children() {
      match c.value() {
        scraper::Node::Text(n) => {
          out.push_str(n);
        }
        scraper::Node::Element(_) => {
          visit(out, ElementRef::wrap(c).unwrap(), emit_link_hrefs);
        }
        _ => {}
      };
    }
    if should_emit_href {
      out.push_str("](");
      out.push_str(elem.attr("href").unwrap());
      out.push_str(")");
    };
    if !INLINE_ELEMS.contains(tag_name) {
      out.push_str("\n\n");
    };
  }
  visit(&mut out, elem, emit_link_hrefs);
  // Remove non-empty blank lines.
  // Collapse whitespace.
  // Trim lines.
  // Reduce multiple line breaks to two.
  out
    .lines()
    .map(|l| l.trim().split_whitespace().join(" "))
    .filter(|l| !l.is_empty())
    .join("\n\n")
}

pub(crate) fn parse_html(html: &str) -> (Meta, String) {
  let mut doc = Html::parse_document(&html);

  let mut meta = Meta::default();
  meta.title = doc
    .select(&*SEL_HEAD_TITLE)
    .next()
    .and_then(|e| e.attr("title"))
    .map(|v| v.to_string());
  meta.lang = doc.root_element().attr("lang").map(|v| v.to_string());
  for elem in doc.select(&*SEL_META) {
    let Some(k) = elem.attr("name").or_else(|| elem.attr("property")) else {
      continue;
    };
    let Some(v) = elem.attr("content") else {
      continue;
    };
    #[rustfmt::skip]
    match k {
      "article:modified_time" => meta.timestamp_modified = DateTime::parse_from_rfc3339(v).map(|v| v.to_utc()).ok(),
      "article:published_time" => meta.timestamp = DateTime::parse_from_rfc3339(v).map(|v| v.to_utc()).ok(),
      "description" => meta.description = Some(v.to_string()),
      "og:description" => meta.description = Some(v.to_string()),
      "og:image" => meta.image_url = Some(v.to_string()),
      "og:locale" => meta.lang = Some(v.to_string()),
      "og:title" => meta.title = Some(v.to_string()),
      "title" => meta.title = Some(v.to_string()),
      "twitter:description" => meta.description = Some(v.to_string()),
      "twitter:image:src" => meta.image_url = Some(v.to_string()),
      "twitter:title" => meta.title = Some(v.to_string()),
      _ => {}
    };
  }

  // https://github.com/causal-agent/scraper/issues/125#issuecomment-1492472021
  let elem_ids_to_remove = doc.select(&*SEL_STRIP).map(|e| e.id()).collect_vec();
  for id in elem_ids_to_remove {
    doc.tree.get_mut(id).unwrap().detach();
  }

  let mut text = doc
    .select(&*SEL_MAIN_ARTICLE)
    .next()
    .or_else(|| doc.select(&*SEL_BODY).next())
    .map(|elem| element_to_text(elem, false))
    .unwrap_or_default();
  text.truncate(64 * 1024);
  let elem_ids_to_remove = doc
    .select(&*SEL_SNIPPET_STRIP)
    .map(|e| e.id())
    .collect_vec();
  for id in elem_ids_to_remove {
    doc.tree.get_mut(id).unwrap().detach();
  }
  meta.snippet = Some(text.chars().take(251).collect());

  (meta, text)
}
