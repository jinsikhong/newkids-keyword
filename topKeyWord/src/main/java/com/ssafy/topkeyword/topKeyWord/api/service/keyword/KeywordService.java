package com.ssafy.topkeyword.topKeyWord.api.service.keyword;

import com.ssafy.topkeyword.topKeyWord.domain.keyword.Keyword;
import com.ssafy.topkeyword.topKeyWord.domain.keyword.repository.KeywordRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import javax.transaction.Transactional;
import javax.validation.constraints.Size;

import java.util.*;


@RequiredArgsConstructor
@Size
@Transactional
@Service
public class KeywordService {
    private final KeywordRepository keywordRepository;

    public Map<String, Keyword> createKeyword(Set<String> keywords){
        List<Keyword> keywordList = new ArrayList<>();
        for(String keyword : keywords){
            keywordList.add(Keyword.builder()
                    .word(keyword)
                    .build());

        }
        keywordRepository.saveAll(keywordList);
        Map<String, Keyword> keywordMap = new HashMap<>();
        for (Keyword keyword : keywordList) {
            keywordMap.put(keyword.getWord(), keyword);
        }

        return keywordMap;
    }
}
