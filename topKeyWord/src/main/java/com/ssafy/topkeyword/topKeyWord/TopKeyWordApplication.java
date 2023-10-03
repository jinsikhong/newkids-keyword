package com.ssafy.topkeyword.topKeyWord;

import com.ssafy.topkeyword.topKeyWord.api.service.article.ArticleQueryService;
import com.ssafy.topkeyword.topKeyWord.api.service.article.KeywordResponse;
import com.ssafy.topkeyword.topKeyWord.api.service.keyword.ArticleKeywordService;
import com.ssafy.topkeyword.topKeyWord.api.service.keyword.KeywordService;
import com.ssafy.topkeyword.topKeyWord.domain.keyword.Keyword;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;

import java.util.Map;

@SpringBootApplication
@Slf4j
public class TopKeyWordApplication {
	private final ArticleQueryService articleQueryService;
	private final KeywordService keywordService;

	private final ArticleKeywordService articleKeywordService;


	@Autowired
	public TopKeyWordApplication(ArticleQueryService articleQueryService, KeywordService keywordService, ArticleKeywordService articleKeywordService) {
		this.articleQueryService = articleQueryService;
		this.keywordService = keywordService;
		this.articleKeywordService = articleKeywordService;
	}


	public static void main(String[] args) {
//		SpringApplication.run(TopKeyWordApplication.class, args);


		log.info("start : start");
		// ArticleQueryService 빈을 주입받아서 메서드 호출
		ApplicationContext context = SpringApplication.run(TopKeyWordApplication.class, args);
		ArticleQueryService articleQueryService = context.getBean(ArticleQueryService.class);
		KeywordService keywordService = context.getBean(KeywordService.class);
		ArticleKeywordService articleKeywordService = context.getBean(ArticleKeywordService.class);

		KeywordResponse keywordResponseToday = articleQueryService.getTopKeywordByToday();
		if(keywordResponseToday == null){
			log.info("수집 데이터 없음");
		}
		log.info("개수");

		// 전체 데이터 넣는 부분
//		KeywordResponse keywordResponse = articleQueryService.getTopKeyword();
//		Map<String, Keyword> keywordIdMap  = keywordService.createKeyword(keywordResponse.getKeywords());
//		articleKeywordService.createArticleKeyword(keywordIdMap, keywordResponse.getTopKeywordResponses());
		log.info("insert 끝");
//		System.out.println(topKeywordResponses.size());

		//
		// topKeywordResponses를 사용하여 원하는 작업 수행
	}

}
