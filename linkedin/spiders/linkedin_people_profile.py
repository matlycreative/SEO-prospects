import json
import scrapy

class LinkedInPeopleProfileSpider(scrapy.Spider):
    name = "linkedin_people_profile"

    def __init__(self, profile_list=None, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Accept:
        # -a profile_list=reidhoffman
        # -a profile_list=reidhoffman,another-slug
        # -a profile_list=["reidhoffman","another-slug"]
        self.profiles = []
        if profile_list:
            s = profile_list.strip()
            if s.startswith("["):
                try:
                    arr = json.loads(s)
                    if isinstance(arr, list):
                        self.profiles = [str(x).strip() for x in arr if str(x).strip()]
                except Exception:
                    self.profiles = [p.strip() for p in s.split(",") if p.strip()]
            else:
                self.profiles = [p.strip() for p in s.split(",") if p.strip()]

    def start_requests(self):
        if not self.profiles:
            self.logger.info("No profile_list provided; nothing to scrape.")
            return

        for profile in self.profiles:
            linkedin_people_url = f"https://www.linkedin.com/in/{profile}/"
            yield scrapy.Request(
                url=linkedin_people_url,
                callback=self.parse_profile,
                meta={"profile": profile, "linkedin_url": linkedin_people_url},
            )

    def parse_profile(self, response):
        item = {
            "profile": response.meta.get("profile", ""),
            "url": response.meta.get("linkedin_url", ""),
        }

        summary_box = response.css("section.top-card-layout")

        item["name"] = (summary_box.css("h1::text").get(default="") or "").strip()
        item["description"] = (summary_box.css("h2::text").get(default="") or "").strip()

        # Location (LinkedIn markup varies)
        loc = summary_box.css("div.top-card__subline-item::text").get(default="")
        loc = (loc or "").strip()
        if not loc:
            loc = (summary_box.css("span.top-card__subline-item::text").get(default="") or "").strip()
        if "followers" in loc.lower() or "connections" in loc.lower():
            loc = ""
        item["location"] = loc

        item["followers"] = ""
        item["connections"] = ""
        for span_text in summary_box.css("span.top-card__subline-item::text").getall():
            t = (span_text or "").strip().lower()
            if "followers" in t:
                item["followers"] = t.replace("followers", "").strip()
            if "connections" in t:
                item["connections"] = t.replace("connections", "").strip()

        item["about"] = (response.css("section.summary div.core-section-container__content p::text").get(default="") or "").strip()

        # Keep it minimal for your Trello enrichment use-case
        yield item
