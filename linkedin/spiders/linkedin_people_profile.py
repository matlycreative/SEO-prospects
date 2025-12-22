import json
import re
import scrapy


class LinkedInPeopleProfileSpider(scrapy.Spider):
    name = "linkedin_people_profile"

    def __init__(self, profile_list=None, *args, **kwargs):
        """
        Accepts:
          -a profile_list=reidhoffman
          -a profile_list=reidhoffman,anotherperson
          -a profile_list=["reidhoffman","anotherperson"]
          -a profile_list=https://www.linkedin.com/in/reidhoffman/
        """
        super().__init__(*args, **kwargs)

        self.profile_list = []

        if profile_list:
            s = str(profile_list).strip()

            # JSON list support
            if s.startswith("[") and s.endswith("]"):
                try:
                    arr = json.loads(s)
                    if isinstance(arr, list):
                        self.profile_list = [str(x).strip() for x in arr if str(x).strip()]
                except Exception:
                    self.profile_list = []
            else:
                # comma-separated
                self.profile_list = [p.strip() for p in s.split(",") if p.strip()]

        if not self.profile_list:
            # fallback if nothing passed
            self.profile_list = ["reidhoffman"]

    def _slug_from_input(self, value: str) -> str:
        v = (value or "").strip()

        # If full URL was provided
        m = re.search(r"linkedin\.com/in/([^/?#]+)", v)
        if m:
            return m.group(1).strip()

        # If just slug was provided
        v = v.strip("/").replace(" ", "")
        return v

    def start_requests(self):
        for raw in self.profile_list:
            slug = self._slug_from_input(raw)
            if not slug:
                continue
            linkedin_people_url = f"https://www.linkedin.com/in/{slug}/"
            yield scrapy.Request(
                url=linkedin_people_url,
                callback=self.parse_profile,
                meta={"profile": slug, "linkedin_url": linkedin_people_url},
            )

    def parse_profile(self, response):
        item = {}
        item["profile"] = response.meta.get("profile")
        item["url"] = response.meta.get("linkedin_url")

        # SUMMARY SECTION
        summary_box = response.css("section.top-card-layout")

        item["name"] = (summary_box.css("h1::text").get(default="") or "").strip()
        item["description"] = (summary_box.css("h2::text").get(default="") or "").strip()

        # Location
        location = (summary_box.css("div.top-card__subline-item::text").get(default="") or "").strip()
        if not location:
            location = (summary_box.css("span.top-card__subline-item::text").get(default="") or "").strip()
        if "followers" in location or "connections" in location:
            location = ""
        item["location"] = location

        item["followers"] = ""
        item["connections"] = ""
        for span_text in summary_box.css("span.top-card__subline-item::text").getall():
            span_text = (span_text or "").strip()
            if "followers" in span_text:
                item["followers"] = span_text.replace(" followers", "").strip()
            if "connections" in span_text:
                item["connections"] = span_text.replace(" connections", "").strip()

        # ABOUT SECTION
        item["about"] = response.css("section.summary div.core-section-container__content p::text").get(default="")

        # EXPERIENCE SECTION
        item["experience"] = []
        experience_blocks = response.css("li.experience-item")
        for block in experience_blocks:
            experience = {}

            try:
                href = block.css("h4 a::attr(href)").get(default="").strip()
                experience["organisation_profile"] = href.split("?")[0] if href else ""
            except Exception:
                experience["organisation_profile"] = ""

            experience["location"] = (block.css("p.experience-item__location::text").get(default="") or "").strip()

            desc = (block.css("p.show-more-less-text__text--more::text").get(default="") or "").strip()
            if not desc:
                desc = (block.css("p.show-more-less-text__text--less::text").get(default="") or "").strip()
            experience["description"] = desc

            experience["start_time"] = ""
            experience["end_time"] = ""
            experience["duration"] = ""

            try:
                date_ranges = block.css("span.date-range time::text").getall()
                date_ranges = [(d or "").strip() for d in date_ranges if (d or "").strip()]
                if len(date_ranges) == 2:
                    experience["start_time"] = date_ranges[0]
                    experience["end_time"] = date_ranges[1]
                    experience["duration"] = (block.css("span.date-range__duration::text").get(default="") or "").strip()
                elif len(date_ranges) == 1:
                    experience["start_time"] = date_ranges[0]
                    experience["end_time"] = "present"
                    experience["duration"] = (block.css("span.date-range__duration::text").get(default="") or "").strip()
            except Exception:
                pass

            item["experience"].append(experience)

        # EDUCATION SECTION
        item["education"] = []
        education_blocks = response.css("li.education__list-item")
        for block in education_blocks:
            education = {}
            education["organisation"] = (block.css("h3::text").get(default="") or "").strip()

            try:
                href = block.css("a::attr(href)").get(default="").strip()
                education["organisation_profile"] = href.split("?")[0] if href else ""
            except Exception:
                education["organisation_profile"] = ""

            course_bits = [t.strip() for t in block.css("h4 span::text").getall() if t and t.strip()]
            education["course_details"] = " ".join(course_bits).strip()

            education["description"] = (block.css("div.education__item--details p::text").get(default="") or "").strip()

            education["start_time"] = ""
            education["end_time"] = ""
            try:
                date_ranges = block.css("span.date-range time::text").getall()
                date_ranges = [(d or "").strip() for d in date_ranges if (d or "").strip()]
                if len(date_ranges) == 2:
                    education["start_time"] = date_ranges[0]
                    education["end_time"] = date_ranges[1]
                elif len(date_ranges) == 1:
                    education["start_time"] = date_ranges[0]
                    education["end_time"] = "present"
            except Exception:
                pass

            item["education"].append(education)

        yield item
