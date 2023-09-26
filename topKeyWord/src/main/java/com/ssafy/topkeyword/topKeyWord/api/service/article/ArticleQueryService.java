package com.ssafy.topkeyword.topKeyWord.api.service.article;

import com.ssafy.topkeyword.topKeyWord.domain.article.repository.ArticleQueryRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.*;

@RequiredArgsConstructor
@Service
@Slf4j
public class ArticleQueryService {
    private final ArticleQueryRepository articleQueryRepository;

    public KeywordResponse getTopKeyword(){
        List<TopKeywordDto> topKeywordList = articleQueryRepository.getTopKeyword();
        KeywordResponse response = splitTopKeyword(topKeywordList);

        return response;
    }


    private KeywordResponse splitTopKeyword(List<TopKeywordDto> dto){
        List<TopKeywordResponse> topKeywordResponses = new ArrayList<>();
        Set<String> keywords = new HashSet<>();
        Set<String> duplicateKey = new HashSet<>();
        Map<String, Integer> duplicateMap = new HashMap<>();
        int keywordCnt = 0;
        int duplicateKeywordCnt = 0;
        for(int i = 0; i < dto.size(); i++){
            String[] articleTopKeywordList = dto.get(i).getTopKeyword().split(" ");

            for(String keyword : articleTopKeywordList){
                keywordCnt++;

                if(!keywords.add(keyword)){
                    duplicateKeywordCnt++;
                    duplicateMap.put(keyword, duplicateMap.getOrDefault(keyword, 0)+1);
                    duplicateKey.add(keyword);
                }
            }

            topKeywordResponses.add(TopKeywordResponse.builder()
                    .articleId(dto.get(i).getArticleId())
                    .topKeywordList(articleTopKeywordList)
                    .build());
        }
        List<String> keySet = new ArrayList<>(duplicateMap.keySet());

        // Value 값으로 오름차순 정렬
        keySet.sort((o1, o2) -> duplicateMap.get(o2).compareTo(duplicateMap.get(o1)));
        log.debug("중복 횟수 순위 리스트");
        for(int z = 0; z < 10; z++){
            System.out.print("중복 횟수 순위 " + (z+1) + "위 : " + keySet.get(z));
            System.out.println(", 중복 횟수 : " + duplicateMap.get(keySet.get(z)));
        }



        log.info("중복 키워드 총 개수 : " + duplicateKey.size());
        log.info("keyword 총 개수  : " + keywordCnt);
        log.info("키워드 중복 횟수   : " + duplicateKeywordCnt);
        return KeywordResponse.builder()
                .topKeywordResponses(topKeywordResponses)
                .keywords(keywords)
                .build();
    }
}
