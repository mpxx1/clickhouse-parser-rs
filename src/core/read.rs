use scraper::{Html, Selector};

pub trait TReadHtml {
    fn read_living_rooms(&self) -> Option<u32>;
    fn read_total_area(&self) -> Option<f64>;
    fn read_living_area(&self) -> Option<f64>;
    fn read_kitchen_area(&self) -> Option<f64>;
    fn read_refit(&self) -> Option<String>;
    fn read_prepayment(&self) -> Option<String>;
    fn read_view_from_windows(&self) -> Option<String>;
    fn read_balconies(&self) -> Option<u32>;
    fn read_floor(&self) -> Option<u32>;
    fn read_total_floors(&self) -> Option<u32>;
    fn read_deposit(&self) -> Option<u32>;
    fn read_utility_bills(&self) -> Option<String>;
    fn read_facilities(&self) -> Vec<String>;
    fn read_metro_stations(&self) -> Vec<(String, String)>;
    fn read_ceiling_height(&self) -> Option<f64>;
    fn read_gaz(&self) -> u32;
    fn read_garbage_chute(&self) -> u32;
    fn read_building_year(&self) -> Option<u32>;
    fn read_walls_material(&self) -> Option<String>;
    fn read_ceilings_type(&self) -> Option<String>;
    fn read_rent(&self) -> Option<u32>;
}

trait TToArea {
    fn to_area(&self) -> Option<f64>;
}

trait TToMins {
    fn to_mins(self) -> String;
}

impl TReadHtml for scraper::Html {
    fn read_living_rooms(&self) -> Option<u32> {
        let Ok(selector) = Selector::parse("li[data-e2e-id='Комнат']") else {
            return None;
        };
        let value_selector = Selector::parse("span[data-e2e-id='Значение']").unwrap();
        let Some(element) = &self.select(&selector).next() else {
            return None;
        };
        let Some(value) = element.select(&value_selector).next() else {
            return None;
        };

        let out = value.to_area();
        if out.is_none() {
            None
        } else {
            Some(out.unwrap() as u32)
        }
    }

    fn read_total_area(&self) -> Option<f64> {
        let Ok(selector) = Selector::parse("li[data-e2e-id='Площадь']") else {
            return None;
        };
        let value_selector = Selector::parse("span[data-e2e-id='Значение']").unwrap();
        let Some(element) = &self.select(&selector).next() else {
            return None;
        };
        let Some(value) = element.select(&value_selector).next() else {
            return None;
        };

        value.to_area()
    }

    fn read_living_area(&self) -> Option<f64> {
        let Ok(selector) = Selector::parse("li[data-e2e-id='Жилая']") else {
            return None;
        };
        let value_selector = Selector::parse("span[data-e2e-id='Значение']").unwrap();
        let Some(element) = &self.select(&selector).next() else {
            return None;
        };
        let Some(value) = element.select(&value_selector).next() else {
            return None;
        };

        value.to_area()
    }

    fn read_kitchen_area(&self) -> Option<f64> {
        let Ok(selector) = Selector::parse("li[data-e2e-id='Кухня']") else {
            return None;
        };
        let value_selector = Selector::parse("span[data-e2e-id='Значение']").unwrap();
        let Some(element) = &self.select(&selector).next() else {
            return None;
        };
        let Some(value) = element.select(&value_selector).next() else {
            return None;
        };

        value.to_area()
    }

    fn read_refit(&self) -> Option<String> {
        let Ok(selector) = Selector::parse("li[data-e2e-id='Ремонт']") else {
            return None;
        };
        let value_selector = Selector::parse("span[data-e2e-id='Значение']").unwrap();
        let Some(element) = &self.select(&selector).next() else {
            return None;
        };
        let Some(val) = element.select(&value_selector).next() else {
            return None;
        };

        let val = match val.text().collect::<Vec<_>>().join("").to_lowercase() {
            x if x.contains("евро") => "euro".to_string(),
            x if x.contains("хороший") => "rnv".to_string(),
            x if x.contains("дизайн") => "design".to_string(),
            x if x.contains("косм") => "cosm".to_string(),
            _ => "no".to_string(),
        };

        match val {
            x if x == "no".to_string() => None,
            _ => Some(val),
        }
    }

    fn read_prepayment(&self) -> Option<String> {
        let Ok(selector) = Selector::parse("li[data-e2e-id='Предоплата']") else {
            return None;
        };
        let value_selector = Selector::parse("span[data-e2e-id='Значение']").unwrap();
        let Some(element) = &self.select(&selector).next() else {
            return None;
        };
        let Some(value) = element.select(&value_selector).next() else {
            return None;
        };

        Some(
            value
                .text()
                .collect::<Vec<_>>()
                .join("")
                .to_lowercase()
                .replace(" мес.", ""),
        )
    }

    fn read_view_from_windows(&self) -> Option<String> {
        let Ok(selector) = Selector::parse("li[data-e2e-id='Вид из окон']") else {
            return None;
        };
        let value_selector = Selector::parse("span[data-e2e-id='Значение']").unwrap();
        let Some(element) = &self.select(&selector).next() else {
            return None;
        };
        let Some(val) = element.select(&value_selector).next() else {
            return None;
        };

        Some(
            match val
                .text()
                .collect::<Vec<_>>()
                .join(" ")
                .replace("\u{a0}", " ")
                .to_lowercase()
            {
                x if x.contains("на улицу") && !x.contains("во двор") => {
                    "street".to_string()
                }
                x if x.contains("во двор") && !x.contains("на улицу") => {
                    "yard".to_string()
                }
                x if x.contains("на улицу") && x.contains("во двор") => {
                    "both".to_string()
                }
                x if x.is_empty() => "".to_string(),
                _ => "street".to_string(),
            },
        )
    }

    fn read_balconies(&self) -> Option<u32> {
        let Ok(selector) = Selector::parse("li[data-e2e-id='Количество балконов']")
        else {
            let Ok(selector) = Selector::parse("li[data-e2e-id='Балкон']") else {
                return None;
            };
            let value_selector = Selector::parse("span[data-e2e-id='Значение']").ok()?;
            let Some(element) = &self.select(&selector).next() else {
                return None;
            };
            let Some(value) = element.select(&value_selector).next() else {
                return None;
            };

            let value = value.text().collect::<Vec<_>>().join("").to_string();

            return if value.contains("больше") {
                Some(4)
            } else {
                value.parse().ok()
            };
        };

        let value_selector = Selector::parse("span[data-e2e-id='Значение']").ok()?;
        let Some(element) = &self.select(&selector).next() else {
            return None;
        };
        let Some(value) = element.select(&value_selector).next() else {
            return None;
        };
        let value = value.text().collect::<Vec<_>>().join("").to_string();

        if value.contains("больше") {
            Some(4)
        } else {
            value.parse().ok()
        }
    }

    fn read_floor(&self) -> Option<u32> {
        let Ok(selector) = Selector::parse("li[data-e2e-id='Этаж']") else {
            return None;
        };

        let value_selector = Selector::parse("span[data-e2e-id='Значение']").ok()?;

        let Some(element) = &self.select(&selector).next() else {
            return None;
        };

        let Some(value) = element.select(&value_selector).next() else {
            return None;
        };

        value
            .text()
            .collect::<Vec<_>>()
            .join(" ")
            .to_string()
            .parse()
            .ok()
    }

    fn read_total_floors(&self) -> Option<u32> {
        let Ok(selector) = Selector::parse("span.bQxSS") else {
            return None;
        };

        for element in self.select(&selector) {
            let text = element.text().collect::<Vec<_>>().join("");

            if text.contains("из") {
                if let Some((_, after)) = text.split_once("из") {
                    let number = after.trim();

                    return number.parse().ok();
                }
            }
        }

        let Ok(selector) = Selector::parse("li[data-e2e-id='Количество этажей']")
        else {
            return None;
        };
        let value_selector = Selector::parse("span[data-e2e-id='Значение']").ok()?;

        let Some(element) = &self.select(&selector).next() else {
            return None;
        };

        let Some(value) = element.select(&value_selector).next() else {
            return None;
        };

        value
            .text()
            .collect::<Vec<_>>()
            .join(" ")
            .to_string()
            .parse()
            .ok()
    }

    fn read_deposit(&self) -> Option<u32> {
        let Ok(selector) = Selector::parse("li[data-e2e-id='Залог']") else {
            return None;
        };

        let value_selector = Selector::parse("span[data-e2e-id='Значение']").ok()?;

        let Some(element) = &self.select(&selector).next() else {
            return None;
        };

        let Some(value) = element.select(&value_selector).next() else {
            return None;
        };

        value
            .text()
            .collect::<Vec<_>>()
            .join(" ")
            .replace(" ", "")
            .replace("₽", "")
            .parse()
            .ok()
    }

    fn read_utility_bills(&self) -> Option<String> {
        let Ok(selector) = Selector::parse("li[data-e2e-id='Коммунальные платежи']")
        else {
            return None;
        };

        let value_selector = Selector::parse("span[data-e2e-id='Значение']").ok()?;

        let Some(element) = &self.select(&selector).next() else {
            return None;
        };

        let Some(val) = element.select(&value_selector).next() else {
            return None;
        };

        let val = match val.text().collect::<Vec<_>>().join(" ").to_lowercase() {
            x if x.contains("не включены") => "no",
            x if x.contains("включены") => "yes",
            x if x.contains("счет") => "by_counters",
            _ => "no",
        }
        .to_string();

        Some(val)
    }

    fn read_facilities(&self) -> Vec<String> {
        let Ok(selector) = Selector::parse("div.lDjMF") else {
            return vec![];
        };
        let mut out = self.select(&selector)
            .map(|x| x.text().collect::<Vec<_>>().join(" ").trim().to_lowercase())
            .collect::<Vec<_>>();

        out
            .retain(|x| {
                !x.contains("охран") &&
                    !x.contains("центр") &&
                    !x.contains("площад") &&
                    !x.contains("сад") &&
                    !x.contains("подземная") &&
                    !x.contains("домофон") &&
                    !x.contains("курить") &&
                    !x.contains("дверь") &&
                    !x.contains("гараж") &&
                    !x.contains("двор") &&
                    !x.contains("без детей") &&
                    !x.contains("нельзя с животными") &&
                    !x.contains("консьерж") &&
                    !x.contains("школа") &&
                    !x.contains("бесплатная") &&
                    !x.contains("наземная") &&
                    !x.contains("шлагба")
            });

        out
    }

    fn read_metro_stations(&self) -> Vec<(String, String)> {
        let Ok(station_selector) = Selector::parse("a.JFWsg.QfBR5") else {
            return vec![];
        };
        let Ok(time_selector) = Selector::parse("span[data-e2e-id='time-on-foot']") else {
            return vec![];
        };

        self.select(&station_selector)
            .zip(self.select(&time_selector))
            .map(|(a, b)| {
                (
                    a.text().collect::<Vec<_>>().join(" ").to_lowercase(),
                    b.text()
                        .collect::<Vec<_>>()
                        .join(" ")
                        .to_lowercase()
                        .replace(" ", "")
                        .replace("\u{a0}", ""),
                )
            })
            .map(|(a, b)| (a, b.to_mins()))
            .collect::<Vec<_>>()
    }

    fn read_ceiling_height(&self) -> Option<f64> {
        let Ok(selector) = Selector::parse("li[data-e2e-id='Высота потолков']")
        else {
            return None;
        };
        let value_selector = Selector::parse("span[data-e2e-id='Значение']").ok()?;
        let Some(element) = &self.select(&selector).next() else {
            return None;
        };
        let Some(value) = element.select(&value_selector).next() else {
            return None;
        };

        value
            .text()
            .collect::<Vec<_>>()
            .join("")
            .trim()
            .to_string()
            .parse()
            .ok()
    }

    fn read_gaz(&self) -> u32 {
        let Ok(selector) = Selector::parse("li[data-e2e-id='Газ']") else {
            return 0;
        };
        let value_selector = Selector::parse("span[data-e2e-id='Значение']").unwrap();
        let Some(element) = &self.select(&selector).next() else {
            return 0;
        };
        let Some(_) = element.select(&value_selector).next() else {
            return 0;
        };

        1
    }

    fn read_garbage_chute(&self) -> u32 {
        let Ok(selector) = Selector::parse("li[data-e2e-id='Мусоропровод']") else {
            return 0;
        };
        let value_selector = Selector::parse("span[data-e2e-id='Значение']").unwrap();
        let Some(element) = &self.select(&selector).next() else {
            return 0;
        };
        let Some(_) = element.select(&value_selector).next() else {
            return 0;
        };

        1
    }

    fn read_building_year(&self) -> Option<u32> {
        let Ok(selector) = Selector::parse(
            "li.ByFq7[data-e2e-id='Год постройки'] span.upbHP[data-e2e-id='Значение']",
        ) else {
            return None;
        };

        if let Some(el) = &self.select(&selector).next() {
            return el
                .text()
                .collect::<Vec<_>>()
                .join("")
                .trim()
                .to_string()
                .parse()
                .ok();
        }

        None
    }

    fn read_walls_material(&self) -> Option<String> {
        let Ok(selector) = Selector::parse(
            "li.ByFq7[data-e2e-id='Материал стен'] span.upbHP[data-e2e-id='Значение']",
        ) else {
            return None;
        };

        comp_material(&self, &selector)
    }

    fn read_ceilings_type(&self) -> Option<String> {
        let Ok(selector) = Selector::parse(
            "li.ByFq7[data-e2e-id='Тип перекрытий'] span.upbHP[data-e2e-id='Значение']",
        ) else {
            return None;
        };

        comp_material(&self, &selector)
    }

    fn read_rent(&self) -> Option<u32> {
        let Ok(selector) = Selector::parse("meta[itemprop='price']") else {
            return None;
        };

        if let Some(element) = &self.select(&selector).next() {
            if let Some(x) = element.value().attr("content") {

                return x.trim().to_string().parse().ok()
            }
        }

        None
    }
}

impl TToArea for scraper::ElementRef<'_> {
    fn to_area(&self) -> Option<f64> {
        Some(
            *&self
                .text()
                .collect::<Vec<_>>()
                .join("")
                .replace("\u{a0}м2", "")
                .replace(',', ".")
                .parse::<f64>()
                .unwrap(),
        )
    }
}

impl TToMins for String {
    fn to_mins(self) -> String {
        if self.contains("мин") {
            return self.replace("мин.", "").parse::<f64>().unwrap().to_string();
        } else if self.contains("час") {
            return (self
                .replace("часа", "")
                .replace("час", "")
                .parse::<f64>()
                .unwrap()
                * 60f64)
                .to_string();
        }

        "---".to_string()
    }
}

fn comp_material(html: &Html, s: &Selector) -> Option<String> {
    if let Some(el) = html.select(s).next() {
        Some(
            match el.text().collect::<Vec<_>>().join("").trim().to_lowercase() {
                x if x.contains("железобетон") => "ferroc".to_string(),
                x if x.contains("панель") && !x.contains("железо") => {
                    "panel".to_string()
                }
                x if x.contains("кирпич") && !x.contains("монолит") => {
                    "brick".to_string()
                }
                x if x.contains("блоч") => "block".to_string(),
                x if x.contains("монолит") && !x.contains("кирпич") => {
                    "mono".to_string()
                }
                x if x.contains("дерев") => "wood".to_string(),
                x if x.contains("бетон") && !x.contains("железо") => "concr".to_string(),
                x if x.contains("смешан")
                    || x.contains("монолитно-кирпичный")
                    || x.contains("кирпично-монолитный")
                    || x.contains("железобетонная панель")
                    || x.contains("ин") =>
                {
                    "mixed".to_string()
                }
                x => x,
            },
        )
    } else {
        None
    }
}
