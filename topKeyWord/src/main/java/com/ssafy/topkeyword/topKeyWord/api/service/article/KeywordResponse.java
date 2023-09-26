package com.ssafy.topkeyword.topKeyWord.api.service.article;

import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Set;

@Data
@NoArgsConstructor
public class KeywordResponse {
    List<TopKeywordResponse> topKeywordResponses;
    Set<String> keywords;

    @Builder
    public KeywordResponse(List<TopKeywordResponse> topKeywordResponses, Set<String> keywords) {
        this.topKeywordResponses = topKeywordResponses;
        this.keywords = keywords;
    }
}
