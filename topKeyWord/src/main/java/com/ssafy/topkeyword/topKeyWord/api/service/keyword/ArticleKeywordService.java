package com.ssafy.topkeyword.topKeyWord.api.service.keyword;

import com.ssafy.topkeyword.topKeyWord.api.service.article.TopKeywordResponse;
import com.ssafy.topkeyword.topKeyWord.domain.keyword.ArticleKeyword;
import com.ssafy.topkeyword.topKeyWord.domain.keyword.Keyword;
import com.ssafy.topkeyword.topKeyWord.domain.keyword.repository.ArticleKeywordRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import javax.transaction.Transactional;
import javax.validation.constraints.Size;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@RequiredArgsConstructor
@Size
@Transactional
@Slf4j
@Service
public class ArticleKeywordService {
    private final ArticleKeywordRepository articleKeywordRepository;

    public void createArticleKeyword(Map<String, Keyword> keywordIdMap, List<TopKeywordResponse> topKeywordResponses) {
        List<ArticleKeyword> articleKeywordList = new ArrayList<>();
        for(int i = 0; i < topKeywordResponses.size(); i++){
            String[] articleKeyword = topKeywordResponses.get(i).getTopKeywordList();
            for(int j = 0; j < articleKeyword.length; j++){
                articleKeywordList.add(ArticleKeyword.builder()
                        .articleKey(topKeywordResponses.get(i).getArticleId())
                        .keyword(keywordIdMap.get(articleKeyword[j]))
                        .build());
            }
        }
        articleKeywordRepository.saveAll(articleKeywordList);
        log.debug("done");
    }


//    public void createArticleKeyword
}
