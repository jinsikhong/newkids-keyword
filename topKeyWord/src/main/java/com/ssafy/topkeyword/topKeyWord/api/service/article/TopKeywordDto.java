package com.ssafy.topkeyword.topKeyWord.api.service.article;

import lombok.Builder;
import lombok.Data;

@Data
public class TopKeywordDto {
    private Long articleId;
    private String topKeyword;

    @Builder
    public TopKeywordDto(Long articleId, String topKeyword) {
        this.articleId = articleId;
        this.topKeyword = topKeyword;
    }
}
